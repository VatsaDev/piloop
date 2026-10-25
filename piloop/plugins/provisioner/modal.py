"""``modal``: each worker of the pool is a Modal Sandbox that runs ``piloop worker`` in the image that you name.

```toml
[plugins.executor.pools.gpu.provisioner.compute]
provider = "modal"
image = "ghcr.io/acme/piloop-worker:0.3"
gpu = "A100"
secrets = ["piloop-cas"]
registry = { kind = "aws-ecr", secret = "piloop-ecr-pull" }
```

The image has ``piloop`` on its ``PATH``, with the evaluator runners and harnesses that the pool's jobs need. Each
sandbox gets the worker's credential in a Modal Secret of its own, never in its command, and the named ``secrets``
beside it. Its tags name its worker, its pool and this server's owner, so ``list`` finds this server's sandboxes alone.

Modal Secrets are for what the sandbox needs before any job runs: the credentials that pull its image (``registry``),
and those of the worker process itself (its CAS's, when the worker reads the bucket). What a job needs, such as a
harness's API key or a gateway URL, comes from piloop's ``secrets`` capability when the job starts, fenced by its
lease, and is never a Modal Secret: a Modal Secret is in the environment of every job on the sandbox.

Modal ends a sandbox after ``timeout_hours`` (24 at most): its lease then expires, and the pool starts another one if a
job still waits.

The server needs Modal's credentials (``MODAL_TOKEN_ID`` and ``MODAL_TOKEN_SECRET``, or ``modal token new``), and the
``modal`` extra (``uv tool install 'piloop[modal]'``), imported when the pool's provisioner opens.
"""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, assert_never

from pydantic import BaseModel, ConfigDict, Field

from piloop.plugins.provisioner.contract import (
    Instance,
    InstanceState,
    Launch,
    ProvisionerConfig,
    ProvisionerHost,
    StartRefused,
)

if TYPE_CHECKING:
    import modal as modal_sdk

OWNER, NAME, POOL = 'piloop-owner', 'piloop-name', 'piloop-pool'
"""The tags of a sandbox."""


class Registry(BaseModel):
    """``registry``: how Modal pulls a private ``image``, with a Modal Secret that only the pull reads."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    kind: Literal['aws-ecr', 'basic']
    """``aws-ecr``: AWS ECR, with ``AWS_ROLE_ARN`` and ``AWS_REGION`` (Modal's OIDC) or an IAM user's keys in the
    Secret. ``basic``: any other registry, with ``REGISTRY_USERNAME`` and ``REGISTRY_PASSWORD``."""
    secret: str
    """The Modal Secret's name, in the provider's ``environment``. It is not in the sandbox's environment."""


class Modal(ProvisionerConfig):
    """``provider = "modal"``: a Modal Sandbox for each worker."""

    image: str
    """The image of each sandbox, from a registry: ``piloop`` is on its ``PATH``."""
    app: str = 'piloop'
    """The Modal app that holds the sandboxes. Made when it is missing."""
    environment: str | None = None
    """The Modal environment. Unset: the default one of the credentials."""
    gpu: str | None = None
    """The GPU of each sandbox, as Modal spells it: ``"A100"``, ``"H100:2"``. Unset: none."""
    cpu: float | None = Field(default=None, gt=0)
    """The CPU cores that each sandbox reserves. Unset: Modal's default."""
    memory_mb: int | None = Field(default=None, gt=0)
    """The MiB of memory that each sandbox reserves. Unset: Modal's default."""
    region: str | None = None
    """Where the sandboxes run, such as ``"us-east"``. Unset: anywhere."""
    timeout_hours: float = Field(default=24, gt=0, le=24)
    """The longest that a sandbox lives."""
    registry: Registry | None = None
    """Where a private ``image`` is pulled from, and with which Secret. Unset: ``image`` is public."""
    secrets: list[str] = []
    """The Modal Secrets that each sandbox gets as its environment, by name: what the sandbox itself needs, never a
    job's secrets (piloop's ``secrets`` capability gives those)."""
    env: dict[str, str] = {}
    """More environment of each sandbox, such as ``PILOOP_PLUGINS__CAS__PROVIDER``: never a secret."""

    def open(self, host: ProvisionerHost) -> ModalProvisioning:
        return ModalProvisioning(self, host.owner)


class ModalProvisioning:
    """``Provisioning``: the sandboxes of the app, tagged with this server's owner."""

    def __init__(self, config: Modal, owner: str) -> None:
        try:  # here, not at the top: the extra is optional, and only a pool on Modal needs it
            import modal
        except ImportError as e:
            raise ImportError(
                "provider = 'modal' needs the modal extra: uv tool install --reinstall 'piloop[modal]'"
            ) from e
        self._modal = modal
        self._config = config
        self._owner = owner
        self._app: modal_sdk.App | None = None

    def app(self) -> modal_sdk.App:
        """The app, looked up on the first call: opening the provisioner touches no network."""
        if self._app is None:
            config = self._config
            self._app = self._modal.App.lookup(config.app, create_if_missing=True, environment_name=config.environment)
        return self._app

    def _sandboxes(self, **tags: str) -> Sequence[modal_sdk.Sandbox]:
        return list(self._modal.Sandbox.list(app_id=self.app().app_id, tags={OWNER: self._owner, **tags}))

    def list(self) -> Sequence[Instance]:
        found = []
        for sandbox in self._sandboxes():
            tags = sandbox.get_tags()
            state = InstanceState.RUNNING if sandbox.poll() is None else InstanceState.ENDED
            found.append(Instance(name=tags[NAME], pool=tags[POOL], state=state, handle={'id': sandbox.object_id}))
        return found

    def start(self, launch: Launch) -> None:
        """Create the sandbox. A running one of its name is the start that came before: nothing to do.

        Raises:
            StartRefused: Modal refused what was asked (an image, a GPU, a secret, a quota).
        """
        modal, config = self._modal, self._config
        credential = modal.Secret.from_dict({k: v.get_secret_value() for k, v in launch.env.items()})
        try:
            modal.Sandbox.create(
                *launch.argv,
                app=self.app(),
                name=launch.name,
                tags={OWNER: self._owner, NAME: launch.name, POOL: launch.pool},
                image=self._image(),
                secrets=[
                    credential,
                    *(modal.Secret.from_name(n, environment_name=config.environment) for n in config.secrets),
                ],
                env=dict(config.env),
                timeout=int(config.timeout_hours * 3600),
                gpu=config.gpu,
                cpu=config.cpu,
                memory=config.memory_mb,
                region=config.region,
            )
        except modal.exception.AlreadyExistsError:
            return
        except (
            modal.exception.InvalidError,
            modal.exception.NotFoundError,
            modal.exception.PermissionDeniedError,
            modal.exception.AuthError,
            modal.exception.ResourceExhaustedError,
            modal.exception.ImageBuildError,
        ) as e:
            raise StartRefused(f'Modal refused the sandbox {launch.name}: {type(e).__name__}: {e}') from e

    def _image(self) -> modal_sdk.Image:
        modal, config = self._modal, self._config
        if (registry := config.registry) is None:
            return modal.Image.from_registry(config.image)
        secret = modal.Secret.from_name(registry.secret, environment_name=config.environment)
        match registry.kind:
            case 'aws-ecr':
                return modal.Image.from_aws_ecr(config.image, secret=secret)
            case 'basic':
                return modal.Image.from_registry(config.image, secret=secret)
            case _:
                assert_never(registry.kind)

    def stop(self, name: str) -> None:
        for sandbox in self._sandboxes(**{NAME: name}):
            sandbox.terminate()

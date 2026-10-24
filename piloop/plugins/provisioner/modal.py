"""``modal``: each worker of the pool is a Modal Sandbox that runs ``piloop worker`` in the image that you name.

```toml
[plugins.executor.pools.gpu.provisioner.compute]
provider = "modal"
image = "ghcr.io/acme/piloop-worker:0.3"
gpu = "A100"
secrets = ["piloop-cas"]
```

The image has ``piloop`` on its ``PATH``, with the evaluator runners and harnesses that the pool's jobs need. Each
sandbox gets the worker's credential in a Modal Secret of its own, never in its command, and the named ``secrets``
(the CAS's credentials, a harness's login) beside it. Its tags name its worker, its pool and this server's owner, so
``list`` finds this server's sandboxes alone. Modal ends a sandbox after ``timeout_hours`` (24 at most): its lease then
expires, and the pool starts another one if a job still waits.

The server needs Modal's credentials (``MODAL_TOKEN_ID`` and ``MODAL_TOKEN_SECRET``, or ``modal token new``), and the
``modal`` extra (``uv tool install 'piloop[modal]'``), imported when the pool's provisioner opens.
"""

from collections.abc import Sequence
from typing import TYPE_CHECKING

from pydantic import Field

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
    secrets: list[str] = []
    """The Modal Secrets that each sandbox gets as its environment, by name."""
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
                image=modal.Image.from_registry(config.image),
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

    def stop(self, name: str) -> None:
        for sandbox in self._sandboxes(**{NAME: name}):
            sandbox.terminate()

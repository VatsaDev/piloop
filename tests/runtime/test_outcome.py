from piloop.plugins.runtime.contract import AgentExit, AgentStatus, ExitStatus, Outcome


def test_the_outcome_of_a_0_2_0_runtime_is_an_agent_exit_and_a_timeout_stays_a_timeout() -> None:
    assert Outcome(AgentStatus.TIMED_OUT, 1, error='agent timeout') == AgentExit(
        ExitStatus.TIMED_OUT, 1, None, 'agent timeout'
    )
    assert Outcome(AgentStatus.IDLE, 2, 0) == AgentExit(ExitStatus.EXITED, 2, 0)

import nox


@nox.session(python=False)
def test(session):
    TEST_COMMANDS = ["typecheck", "test"]
    for x in TEST_COMMANDS:
        session.run("pipenv", "run", x)

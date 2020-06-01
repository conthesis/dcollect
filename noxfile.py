import nox


@nox.session
def tests(session):
    session.install("pytest")
    session.run("pytest")


@nox.session(python=False)
def fmt(session):
    FMT_COMMANDS = ["fix_flake", "fmt_imports", "fmt_black"]
    for x in FMT_COMMANDS:
        session.run("pipenv", "run", x)


@nox.session(python=False)
def test(session):
    TEST_COMMANDS = ["typecheck", "test"]
    for x in TEST_COMMANDS:
        session.run("pipenv", "run", x)

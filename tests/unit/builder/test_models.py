from genie_world.builder import BuilderWarning, BuildResult


class TestBuilderModels:
    def test_builder_warning(self):
        w = BuilderWarning(section="example_sqls", message="SQL validation failed", detail="SELECT bad")
        assert w.section == "example_sqls"

    def test_build_result(self):
        r = BuildResult(config={"version": 2}, warnings=[])
        assert r.config["version"] == 2
        assert len(r.warnings) == 0

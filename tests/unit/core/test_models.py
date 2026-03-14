from genie_world.core.models import SpaceConfig


class TestSpaceConfig:
    def test_minimal(self):
        config = SpaceConfig(display_name="Sales Analytics")
        assert config.display_name == "Sales Analytics"
        assert config.data_sources is None
        assert config.instructions is None
        assert config.benchmarks is None

    def test_serialization(self):
        config = SpaceConfig(display_name="Test Space")
        json_str = config.model_dump_json()
        loaded = SpaceConfig.model_validate_json(json_str)
        assert loaded.display_name == "Test Space"

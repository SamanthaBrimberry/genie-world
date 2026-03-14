from genie_world.core.tracing import trace


class TestTraceDecorator:
    def test_decorated_function_runs(self):
        @trace
        def add(a, b):
            return a + b
        assert add(1, 2) == 3

    def test_decorated_with_args(self):
        @trace(name="custom_name", span_type="PARSER")
        def parse(data):
            return data.upper()
        assert parse("hello") == "HELLO"

    def test_preserves_function_name(self):
        @trace
        def my_func():
            pass
        my_func()

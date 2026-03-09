"""Tests for the hook system."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from crm_medallion.bronze.ingester import CSVIngester
from crm_medallion.bronze.models import BronzeDataset
from crm_medallion.config.framework_config import BronzeConfig, SilverConfig, GoldConfig
from crm_medallion.utils.hooks import (
    Hook,
    HookContext,
    HookExecutor,
    HookPhase,
    HookRegistry,
    HookResponse,
    HookResult,
    FunctionHook,
    create_hook,
    get_global_registry,
    register_hook,
)


class TestHookRegistry:
    def test_register_hook_instance(self):
        registry = HookRegistry()

        class TestHook(Hook[str]):
            @property
            def name(self) -> str:
                return "test_hook"

            def execute(self, context: HookContext[str]) -> HookResponse[str]:
                return HookResponse(result=HookResult.CONTINUE)

        hook = TestHook()
        registry.register("bronze", HookPhase.PRE, hook)

        hooks = registry.get_hooks("bronze", HookPhase.PRE)
        assert len(hooks) == 1
        assert hooks[0].name == "test_hook"

    def test_register_callable(self):
        registry = HookRegistry()

        def my_hook(context: HookContext) -> HookResponse:
            return HookResponse(result=HookResult.CONTINUE)

        registry.register("silver", HookPhase.POST, my_hook)

        hooks = registry.get_hooks("silver", HookPhase.POST)
        assert len(hooks) == 1
        assert hooks[0].name == "my_hook"

    def test_unregister_hook(self):
        registry = HookRegistry()

        def my_hook(context):
            return HookResponse(result=HookResult.CONTINUE)

        registry.register("bronze", HookPhase.PRE, my_hook)
        assert len(registry.get_hooks("bronze", HookPhase.PRE)) == 1

        removed = registry.unregister("bronze", HookPhase.PRE, "my_hook")
        assert removed is True
        assert len(registry.get_hooks("bronze", HookPhase.PRE)) == 0

    def test_unregister_nonexistent_hook(self):
        registry = HookRegistry()

        removed = registry.unregister("bronze", HookPhase.PRE, "nonexistent")
        assert removed is False

    def test_get_hooks_empty(self):
        registry = HookRegistry()

        hooks = registry.get_hooks("bronze", HookPhase.PRE)
        assert hooks == []

    def test_clear_all(self):
        registry = HookRegistry()

        def hook1(ctx):
            return None

        def hook2(ctx):
            return None

        registry.register("bronze", HookPhase.PRE, hook1)
        registry.register("silver", HookPhase.POST, hook2)

        registry.clear()

        assert registry.get_hooks("bronze", HookPhase.PRE) == []
        assert registry.get_hooks("silver", HookPhase.POST) == []

    def test_clear_by_layer(self):
        registry = HookRegistry()

        def hook1(ctx):
            return None

        def hook2(ctx):
            return None

        registry.register("bronze", HookPhase.PRE, hook1)
        registry.register("bronze", HookPhase.POST, hook1)
        registry.register("silver", HookPhase.POST, hook2)

        registry.clear(layer="bronze")

        assert registry.get_hooks("bronze", HookPhase.PRE) == []
        assert registry.get_hooks("bronze", HookPhase.POST) == []
        assert len(registry.get_hooks("silver", HookPhase.POST)) == 1

    def test_clear_by_phase(self):
        registry = HookRegistry()

        def hook1(ctx):
            return None

        registry.register("bronze", HookPhase.PRE, hook1)
        registry.register("silver", HookPhase.PRE, hook1)
        registry.register("bronze", HookPhase.POST, hook1)

        registry.clear(phase=HookPhase.PRE)

        assert registry.get_hooks("bronze", HookPhase.PRE) == []
        assert registry.get_hooks("silver", HookPhase.PRE) == []
        assert len(registry.get_hooks("bronze", HookPhase.POST)) == 1


class TestHookExecutor:
    def test_execute_no_hooks(self):
        registry = HookRegistry()
        executor = HookExecutor(registry)

        result, data = executor.execute_hooks("bronze", HookPhase.PRE, "test_data")

        assert result == HookResult.CONTINUE
        assert data == "test_data"

    def test_execute_hook_modifies_data(self):
        registry = HookRegistry()

        def modify_hook(context: HookContext) -> HookResponse:
            return HookResponse(
                result=HookResult.CONTINUE,
                data=context.data.upper(),
            )

        registry.register("bronze", HookPhase.PRE, modify_hook)
        executor = HookExecutor(registry)

        result, data = executor.execute_hooks("bronze", HookPhase.PRE, "test_data")

        assert result == HookResult.CONTINUE
        assert data == "TEST_DATA"

    def test_execute_hook_skip(self):
        registry = HookRegistry()

        def skip_hook(context: HookContext) -> HookResponse:
            return HookResponse(result=HookResult.SKIP, message="Skipping")

        registry.register("silver", HookPhase.PRE, skip_hook)
        executor = HookExecutor(registry)

        result, data = executor.execute_hooks("silver", HookPhase.PRE, "test")

        assert result == HookResult.SKIP

    def test_execute_hook_abort(self):
        registry = HookRegistry()

        def abort_hook(context: HookContext) -> HookResponse:
            return HookResponse(result=HookResult.ABORT, message="Aborting")

        registry.register("gold", HookPhase.POST, abort_hook)
        executor = HookExecutor(registry)

        result, data = executor.execute_hooks("gold", HookPhase.POST, "test")

        assert result == HookResult.ABORT

    def test_execute_multiple_hooks_in_order(self):
        registry = HookRegistry()
        call_order = []

        def hook1(context: HookContext) -> HookResponse:
            call_order.append("hook1")
            return HookResponse(
                result=HookResult.CONTINUE,
                data=context.data + "_1",
            )

        def hook2(context: HookContext) -> HookResponse:
            call_order.append("hook2")
            return HookResponse(
                result=HookResult.CONTINUE,
                data=context.data + "_2",
            )

        registry.register("bronze", HookPhase.PRE, hook1)
        registry.register("bronze", HookPhase.PRE, hook2)
        executor = HookExecutor(registry)

        result, data = executor.execute_hooks("bronze", HookPhase.PRE, "start")

        assert call_order == ["hook1", "hook2"]
        assert data == "start_1_2"

    def test_execute_hook_with_metadata(self):
        registry = HookRegistry()
        captured_metadata = {}

        def capture_metadata(context: HookContext) -> HookResponse:
            captured_metadata.update(context.metadata)
            return HookResponse(result=HookResult.CONTINUE)

        registry.register("bronze", HookPhase.PRE, capture_metadata)
        executor = HookExecutor(registry)

        executor.execute_hooks(
            "bronze",
            HookPhase.PRE,
            "data",
            metadata={"key": "value"},
        )

        assert captured_metadata == {"key": "value"}


class TestFunctionHook:
    def test_creates_from_function(self):
        def my_func(context):
            return HookResponse(result=HookResult.CONTINUE)

        hook = FunctionHook(my_func)

        assert hook.name == "my_func"

    def test_creates_with_custom_name(self):
        def my_func(context):
            return None

        hook = FunctionHook(my_func, name="custom_name")

        assert hook.name == "custom_name"

    def test_execute_returns_hook_response(self):
        def my_func(context):
            return HookResponse(result=HookResult.SKIP)

        hook = FunctionHook(my_func)
        context = HookContext(
            data="test",
            layer="bronze",
            phase=HookPhase.PRE,
            metadata={},
        )

        response = hook.execute(context)

        assert response.result == HookResult.SKIP

    def test_execute_with_none_return(self):
        def my_func(context):
            return None

        hook = FunctionHook(my_func)
        context = HookContext(
            data="test",
            layer="bronze",
            phase=HookPhase.PRE,
            metadata={},
        )

        response = hook.execute(context)

        assert response.result == HookResult.CONTINUE
        assert response.data is None

    def test_execute_with_data_return(self):
        def my_func(context):
            return "modified_data"

        hook = FunctionHook(my_func)
        context = HookContext(
            data="test",
            layer="bronze",
            phase=HookPhase.PRE,
            metadata={},
        )

        response = hook.execute(context)

        assert response.result == HookResult.CONTINUE
        assert response.data == "modified_data"


class TestHookContext:
    def test_with_data_creates_new_context(self):
        context = HookContext(
            data="original",
            layer="bronze",
            phase=HookPhase.PRE,
            metadata={"key": "value"},
        )

        new_context = context.with_data("new_data")

        assert new_context.data == "new_data"
        assert new_context.layer == "bronze"
        assert new_context.phase == HookPhase.PRE
        assert new_context.metadata == {"key": "value"}
        assert context.data == "original"


class TestCreateHookDecorator:
    def test_decorator_creates_function_hook(self):
        @create_hook(name="my_hook")
        def my_func(context):
            return HookResponse(result=HookResult.CONTINUE)

        assert isinstance(my_func, FunctionHook)
        assert my_func.name == "my_hook"


class TestGlobalRegistry:
    def test_get_global_registry(self):
        registry = get_global_registry()
        assert isinstance(registry, HookRegistry)

    def test_register_hook_globally(self):
        registry = get_global_registry()
        registry.clear()

        def test_hook(context):
            return None

        register_hook("bronze", HookPhase.PRE, test_hook)

        hooks = registry.get_hooks("bronze", HookPhase.PRE)
        assert len(hooks) == 1

        registry.clear()


class TestHooksIntegrationWithLayers:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sample_csv(self, temp_dir):
        csv_path = temp_dir / "test.csv"
        csv_path.write_text(
            "num_factura,fecha,proveedor\n"
            "FAC-001,2024-01-15,Empresa A\n"
        )
        return csv_path

    def test_bronze_ingester_with_pre_hook(self, temp_dir, sample_csv):
        registry = HookRegistry()
        captured_data = {}

        def capture_hook(context: HookContext) -> HookResponse:
            captured_data["path"] = str(context.data)
            captured_data["phase"] = context.phase
            return HookResponse(result=HookResult.CONTINUE)

        registry.register("bronze", HookPhase.PRE, capture_hook)

        config = BronzeConfig(storage_path=temp_dir / "bronze")
        ingester = CSVIngester(config=config, hook_registry=registry)

        ingester.ingest(sample_csv)

        assert captured_data["path"] == str(sample_csv)
        assert captured_data["phase"] == HookPhase.PRE

    def test_bronze_ingester_with_post_hook(self, temp_dir, sample_csv):
        registry = HookRegistry()
        captured_dataset = None

        def capture_hook(context: HookContext) -> HookResponse:
            nonlocal captured_dataset
            captured_dataset = context.data
            return HookResponse(result=HookResult.CONTINUE)

        registry.register("bronze", HookPhase.POST, capture_hook)

        config = BronzeConfig(storage_path=temp_dir / "bronze")
        ingester = CSVIngester(config=config, hook_registry=registry)

        result = ingester.ingest(sample_csv)

        assert captured_dataset is not None
        assert isinstance(captured_dataset, BronzeDataset)
        assert captured_dataset.id == result.id

    def test_bronze_ingester_hook_skip(self, temp_dir, sample_csv):
        registry = HookRegistry()

        def skip_hook(context: HookContext) -> HookResponse:
            return HookResponse(result=HookResult.SKIP)

        registry.register("bronze", HookPhase.PRE, skip_hook)

        config = BronzeConfig(storage_path=temp_dir / "bronze")
        ingester = CSVIngester(config=config, hook_registry=registry)

        result = ingester.ingest(sample_csv)

        assert result.row_count == 0
        assert result.metadata.get("skipped") is True

    def test_bronze_ingester_hook_abort(self, temp_dir, sample_csv):
        registry = HookRegistry()

        def abort_hook(context: HookContext) -> HookResponse:
            return HookResponse(result=HookResult.ABORT)

        registry.register("bronze", HookPhase.PRE, abort_hook)

        config = BronzeConfig(storage_path=temp_dir / "bronze")
        ingester = CSVIngester(config=config, hook_registry=registry)

        from crm_medallion.utils.errors import FrameworkError

        with pytest.raises(FrameworkError) as exc_info:
            ingester.ingest(sample_csv)

        assert "aborted by hook" in str(exc_info.value)

    def test_ingester_without_hooks(self, temp_dir, sample_csv):
        config = BronzeConfig(storage_path=temp_dir / "bronze")
        ingester = CSVIngester(config=config)

        result = ingester.ingest(sample_csv)

        assert result.row_count == 1

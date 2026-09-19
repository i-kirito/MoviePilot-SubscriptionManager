import ast
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "plugins.v2" / "subscriptionmanager" / "__init__.py"


class SubscriptionManagerMetadataTests(unittest.TestCase):
    def test_package_metadata_matches_plugin(self):
        package = json.loads((ROOT / "package.v2.json").read_text(encoding="utf-8"))
        entry = package["SubscriptionManager"]
        source = PLUGIN.read_text(encoding="utf-8")
        tree = ast.parse(source)
        class_node = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "SubscriptionManager"
        )
        class_values = {
            node.targets[0].id: ast.literal_eval(node.value)
            for node in class_node.body
            if isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id in {"plugin_version", "plugin_author"}
        }
        self.assertEqual(entry["version"], class_values["plugin_version"])
        self.assertEqual(entry["author"], class_values["plugin_author"])
        self.assertTrue((ROOT / "icons" / "subscriptionmanager.png").is_file())

    def test_page_contains_dashboard_sections(self):
        source = PLUGIN.read_text(encoding="utf-8")
        tree = ast.parse(source)
        get_page = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "get_page"
        )
        page_source = ast.get_source_segment(source, get_page)
        self.assertIsNotNone(page_source)
        for label in ("运行概况", "最近自动订阅", "待处理提醒", "转移记录清理", "metric_card"):
            self.assertIn(label, page_source)


if __name__ == "__main__":
    unittest.main()

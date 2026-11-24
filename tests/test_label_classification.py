from importlib.util import find_spec
import unittest

if any(find_spec(dependency) is None for dependency in ("numpy", "pandas", "matplotlib")):
    raise unittest.SkipTest("Run the Veracity experiment tests on the host with pandas and matplotlib installed")

from experiments.classify_labels import classify_label, group_counts


class LabelClassificationTests(unittest.TestCase):
    def test_known_labels_map_to_documented_families(self):
        cases = {
            " BENIGN ": "BENIGN",
            "DoS Hulk": "DOS",
            "DDoS": "DDOS",
            "PortScan": "PORT_SCAN",
            "FTP-Patator": "BRUTE_FORCE",
            "SSH-Patator": "BRUTE_FORCE",
            "Web Attack \ufffd XSS": "WEB_ATTACK",
            "Bot": "BOTNET",
            "Infiltration": "INFILTRATION",
            "Heartbleed": "HEARTBLEED",
            "new-label": "UNKNOWN",
        }
        for label, expected in cases.items():
            with self.subTest(label=label):
                self.assertEqual(classify_label(label), expected)

    def test_group_counts_preserves_record_total(self):
        labels = {"BENIGN": 10, "DoS Hulk": 3, "FTP-Patator": 2, "Web Attack \ufffd XSS": 1}
        grouped = group_counts(labels)
        self.assertEqual(sum(grouped.values()), sum(labels.values()))
        self.assertEqual(grouped["BENIGN"], 10)
        self.assertEqual(grouped["DOS"], 3)
        self.assertEqual(grouped["BRUTE_FORCE"], 2)
        self.assertEqual(grouped["WEB_ATTACK"], 1)


if __name__ == "__main__":
    unittest.main()

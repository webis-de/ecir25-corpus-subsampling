import unittest

from approvaltests import verify_as_json

from corpus_subsampling.run_files import IR_DATASET_IDS, Runs


class TestAssignmentOfRunsToGroups(unittest.TestCase):
    def test_assignment_of_runs_to_groups_on_robust04(self):
        run_file_groups = Runs("disks45/nocr/trec-robust-2004")

        expected = {"juru": ["trec-system-runs/trec13/robust/input.Juru.asd.gz"]}
        actual = run_file_groups.assign_runs_to_groups(["trec-system-runs/trec13/robust/input.Juru.asd.gz"])

        self.assertEqual(expected, actual)

    def test_assignment_of_runs_to_groups_fails_on_unknown_runs_for_robust04(self):
        run_file_groups = Runs("disks45/nocr/trec-robust-2004")
        expected = "cant determine a group for trec-system-runs/trec13/robust/input.Juhu.asd.gz"

        with self.assertRaises(Exception) as context:
            run_file_groups.assign_runs_to_groups(["trec-system-runs/trec13/robust/input.Juhu.asd.gz"])

        self.assertTrue(expected in str(context.exception))

    def test_all_runs(self):
        actual = {}
        for i in IR_DATASET_IDS:
            tmp = {}
            for k, v in Runs(i).all_runs().items():
                # normalize paths for reproducible tests
                v = [j.split("trec-system-runs")[1] for j in v]
                tmp[k] = v

            actual[i] = tmp

        verify_as_json(actual)

"""Joint verification entry point used by the runtime manifest.

Runs the unchanged regression suite, then asserts the cross-partition transfer
contract from MOVE_MIRROR.md: transfer first-execution replay, stale old image,
contradictory bindings, artifact regression and both delivery orders.  Exits
non-zero on any failure.
"""

import sys
import unittest

from examples.transfer_case import transfer_report


def run_regression() -> bool:
    suite = unittest.defaultTestLoader.discover("tests", top_level_dir=".")
    if not suite.countTestCases():
        print("No tests discovered in tests/", file=sys.stderr)
        return False
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    for status, cases in (("FAIL", (test for test, _ in result.failures)),
                          ("ERROR", (test for test, _ in result.errors)),
                          ("UNEXPECTED_SUCCESS", result.unexpectedSuccesses)):
        for case in cases:
            print(f"{status} {case.id()}")
    print(f"TEST SUMMARY total={result.testsRun} failures={len(result.failures)} "
          f"errors={len(result.errors)} skipped={len(result.skipped)} "
          f"result={'PASS' if result.wasSuccessful() else 'FAIL'}")
    return result.wasSuccessful() and result.testsRun > 0


def run_transfer_checks() -> bool:
    try:
        report = transfer_report()
    except Exception as error:  # noqa: BLE001 - report any drill failure as a check failure
        print(f"TRANSFER CHECK FAIL {type(error).__name__}: {error}", file=sys.stderr)
        return False
    expected = report["expected"]["complete"]
    new_first = report["new_partition_first"]
    original_first = report["original_partition_first"]
    checks = {
        "new_partition_only_order": new_first["after_first"]["甲"]["rows"] == [7, 2],
        "original_partition_only_order": original_first["after_first"]["甲"]["rows"] == [8, 3],
        "both_orders_complete": (
            new_first["after_both"]["甲"]["rows"] == expected["甲"]
            and new_first["after_both"]["乙"]["rows"] == expected["乙"]
            and original_first["after_both"]["甲"]["rows"] == expected["甲"]
            and original_first["after_both"]["乙"]["rows"] == expected["乙"]
        ),
        "fixed_accounts_preserved": (
            new_first["a2_account"]["rows"] == [7, 2] and original_first["a2_account"]["rows"] == [7, 2]
            and new_first["b3_account"]["rows"] == [9, 4]
        ),
        "commit_retry": report["commit_failure_retry"]["status_after_failure"] == "pending",
    }
    for name, passed in checks.items():
        print(f"TRANSFER CHECK {name} {'PASS' if passed else 'FAIL'}")
    return all(checks.values())


def main() -> int:
    regression = run_regression()
    transfer = run_transfer_checks()
    return 0 if regression and transfer else 1


if __name__ == "__main__":
    raise SystemExit(main())

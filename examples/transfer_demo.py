"""Runtime demo for the cross-partition transfer hand-off.

Prints the formal ledgers before and after each arrival for both delivery
orders, plus the commit-failure retry, in a compact human readable form.
"""

import json

from examples.transfer_case import transfer_report


def _line(label, observation):
    alpha = observation["甲"]
    beta = observation["乙"]
    return (f"{label}: 甲={alpha['rows']} ({alpha['count']}笔/{alpha['liters']}L) "
            f"乙={beta['rows']} ({beta['count']}笔/{beta['liters']}L) "
            f"转记={observation['status']}")


def main():
    report = transfer_report()
    print("== 新区件先到（B3 → A5） ==")
    case = report["new_partition_first"]
    print(_line("初装 A2+B0", case["before"]))
    print(_line("收 B3   ", case["after_first"]))
    print(_line("再收 A5 ", case["after_both"]))
    print(_line("迟收 B1 ", case["after_late_b1"]))
    print(f"重开镜像: {_line('断源重开', case['after_reopen'])}")
    print(f"原发行 A2: {case['a2_account']['rows']} / B3: {case['b3_account']['rows']}")

    print("== 原区件先到（A5 → B3） ==")
    case = report["original_partition_first"]
    print(_line("初装 A2+B0", case["before"]))
    print(_line("收 A5   ", case["after_first"]))
    print(_line("再收 B3 ", case["after_both"]))
    print(_line("迟收 B1 ", case["after_late_b1"]))
    print(f"重开镜像: {_line('断源重开', case['after_reopen'])}")
    print(f"原发行 A2: {case['a2_account']['rows']} / B3: {case['b3_account']['rows']}")

    print("== 提交失败与重送 ==")
    retry = report["commit_failure_retry"]
    print(f"收 B3 后: {_line('', retry['before'])}")
    print(f"注入提交失败: {retry['failure']}")
    print(_line("失败后重开", retry["after_failure"]))
    print(f"重送 A5 结果: {retry['resend']}")
    print(_line("重送后    ", retry["after_resend"]))
    print("DEMO OK " + json.dumps(report["expected"]["complete"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# -*- coding: utf-8 -*-
"""
QA 深度验证器（L3）— 用 DB 数据验证 AI 回答的真实性

核心能力：
1. 实体存在性：回答中的服务区名是否在 DB 中存在
2. 片区归属：问"皖北"时，回答中的 SA 是否真属于皖北
3. 营收数字核对：回答中的数字与 dameng_mirror.db 对比
4. 跨轮一致性：多轮对话中同一 SA 数字不矛盾

用法：
    # 作为模块被 qa_runner.py 调用
    from qa_verifier import DeepVerifier
    verifier = DeepVerifier()
    result = verifier.verify(question, report)

    # 独立运行，对已有 JSON 做事后核对
    python qa/qa_verifier.py --input qa/qa_multi_multi_v2.json
"""

import json
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Windows 控制台 UTF-8
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 数据库路径：优先相对路径（跨平台），fallback Windows 绝对路径
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_DATA_CANDIDATES = [
    _PROJECT_ROOT / "data",            # ../.. 相对路径（Linux/macOS）
    Path("d:/AISpace/AI-Python/data"),  # Windows 绝对路径
]
_DATA_DIR = next((d for d in _DATA_CANDIDATES if d.exists()), _DATA_CANDIDATES[0])
_MAIN_DB = _DATA_DIR / "db.sqlite3"
_MIRROR_DB = _DATA_DIR / "dameng_mirror.db"


# ============================================================
# 数据结构
# ============================================================

@dataclass
class Issue:
    """验证发现的问题"""
    level: str           # 🔴 幻觉 / 🟡 存疑 / ℹ️ 信息
    category: str        # entity / region / revenue / consistency
    message: str         # 人类可读的描述
    detail: dict = field(default_factory=dict)  # 结构化数据（供程序消费）


@dataclass
class L3Result:
    """L3 验证结果"""
    issues: List[Issue] = field(default_factory=list)
    entities_found: List[str] = field(default_factory=list)  # 回答中提到的 SA
    numbers_found: Dict[str, float] = field(default_factory=dict)  # SA→金额

    @property
    def has_errors(self) -> bool:
        return any(i.level == "🔴" for i in self.issues)

    @property
    def has_warnings(self) -> bool:
        return any(i.level == "🟡" for i in self.issues)

    def summary(self) -> str:
        if not self.issues:
            return "✅"
        parts = []
        for i in self.issues:
            parts.append(f"{i.level} {i.message}")
        return "; ".join(parts)


# ============================================================
# 深度验证器
# ============================================================

class DeepVerifier:
    """L3 深度验证器"""

    def __init__(self, db_path: str = None, mirror_db_path: str = None):
        self.db_path = db_path or str(_MAIN_DB)
        self.mirror_db_path = mirror_db_path or str(_MIRROR_DB)

        # 加载基础数据（一次性）
        self._sa_names: set = set()           # 所有服务区名称
        self._sa_short_names: set = set()     # 去掉"服务区"后缀
        self._sa_region: Dict[str, str] = {}  # SA名 → 片区名
        self._region_sas: Dict[str, set] = {} # 片区名 → SA 名集合
        self._sa_ids: Dict[str, int] = {}     # SA名 → original_id
        self._brands: set = set()             # 品牌名
        self._mirror_available = False        # dameng_mirror.db 是否可用
        self._mirror_revenue: Dict[str, Dict[str, float]] = {}  # SA名→{month→金额}

        self._load_base_data()
        self._load_mirror_data()

    def _load_base_data(self):
        """从 db.sqlite3 加载基础映射数据"""
        if not Path(self.db_path).exists():
            print(f"⚠️ 主数据库不存在: {self.db_path}")
            return

        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # 服务区 → 片区
        c.execute("""
            SELECT sa.name, r.name, sa.original_id
            FROM api_servicearea sa
            JOIN api_region r ON sa.region_id = r.id
        """)
        for sa_name, region_name, original_id in c.fetchall():
            self._sa_names.add(sa_name)
            short = sa_name.replace("服务区", "")
            self._sa_short_names.add(short)
            self._sa_region[sa_name] = region_name
            self._sa_region[short] = region_name  # 也支持短名查询
            if region_name not in self._region_sas:
                self._region_sas[region_name] = set()
            self._region_sas[region_name].add(sa_name)
            if original_id:
                self._sa_ids[sa_name] = original_id

        # 品牌
        c.execute("SELECT name FROM api_brand")
        for (name,) in c.fetchall():
            self._brands.add(name)

        conn.close()
        print(f"  📊 基础数据: {len(self._sa_names)} 服务区, "
              f"{len(self._region_sas)} 片区, {len(self._brands)} 品牌")

    def _load_mirror_data(self):
        """从 dameng_mirror.db 加载营收真实数据（按月聚合）"""
        if not Path(self.mirror_db_path).exists():
            print(f"  ℹ️ 镜像数据库不存在: {self.mirror_db_path}（跳过营收核对）")
            return

        try:
            conn = sqlite3.connect(self.mirror_db_path)
            c = conn.cursor()

            # --- 1. 按月聚合对客销售（SA级月度数据，核心数据源）---
            # 结构: {SA名: {月份: 对客销售(元)}}
            c.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type='table' AND name='NEWGETREVENUEREPORT_SHOPS'"
            )
            if c.fetchone()[0] > 0:
                c.execute(
                    "SELECT [服务区名称], STATISTICS_MONTH, SUM([对客销售]) "
                    "FROM NEWGETREVENUEREPORT_SHOPS "
                    "WHERE [服务区名称] IS NOT NULL AND [服务区名称] != '汇总' "
                    "GROUP BY [服务区名称], STATISTICS_MONTH"
                )
                for sa, month, total in c.fetchall():
                    if sa and total and month:
                        if sa not in self._mirror_revenue:
                            self._mirror_revenue[sa] = {}
                        try:
                            self._mirror_revenue[sa][str(month)] = float(total)
                        except (ValueError, TypeError):
                            pass

            # --- 2. 排名数据（年度累计排行）---
            # 结构: [{SA名, 累计对客销售, 排名}, ...]
            self._ranking_data = []
            c.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type='table' AND name='NEWREVENUERANKING'"
            )
            if c.fetchone()[0] > 0:
                c.execute(
                    "SELECT [服务区名称], [累计对客销售], [累计对客销售排行] "
                    "FROM NEWREVENUERANKING "
                    "WHERE [服务区名称] IS NOT NULL"
                )
                for sa, amount, rank in c.fetchall():
                    if sa and amount:
                        try:
                            self._ranking_data.append({
                                "name": sa,
                                "amount": float(amount),
                                "rank": int(rank) if rank else 0,
                            })
                        except (ValueError, TypeError):
                            pass
                self._ranking_data.sort(key=lambda x: x["amount"], reverse=True)

            # --- 3. 非实体黑名单（从 config/constants.py 加载）---
            self._blacklist_names = set()
            try:
                sa_db = sqlite3.connect(self.db_path)
                sa_c = sa_db.cursor()
                # 查 area_type 为空的 SA（非实体）
                sa_c.execute(
                    "SELECT name FROM api_servicearea "
                    "WHERE area_type IS NULL OR area_type = ''"
                )
                for (name,) in sa_c.fetchall():
                    if name:
                        self._blacklist_names.add(name)
                sa_db.close()
            except Exception:
                pass

            if self._mirror_revenue:
                self._mirror_available = True
                months = set()
                for data in self._mirror_revenue.values():
                    months.update(data.keys())
                months_str = ", ".join(sorted(months)[-3:])
                print(f"  📊 镜像数据: {len(self._mirror_revenue)} SA, "
                      f"{len(self._ranking_data)} 排名, "
                      f"月份: {months_str}等")
            else:
                print("  ℹ️ 镜像数据库无可用营收数据")

            conn.close()
        except Exception as e:
            print(f"  ⚠️ 加载镜像数据失败: {e}")

    # ============================================================
    # 文本提取工具
    # ============================================================

    def extract_service_areas(self, text: str) -> List[str]:
        """从文本中提取提到的服务区名称"""
        found = []
        # 优先匹配完整名称 "XX服务区"
        for sa in self._sa_names:
            if sa in text:
                found.append(sa)

        # 如果没匹配到完整名，尝试短名 + "服务区" 后缀
        if not found:
            for short in sorted(self._sa_short_names, key=len, reverse=True):
                if len(short) >= 2 and short in text:
                    full = short + "服务区"
                    if full in self._sa_names and full not in found:
                        found.append(full)

        return found

    def extract_revenue_numbers(self, text: str) -> Dict[str, List[float]]:
        """
        从文本中提取 "XX服务区...XXX万元" 格式的营收数字
        返回 {服务区名: [数字列表]}
        """
        result: Dict[str, List[float]] = {}

        # 策略：找到服务区名后，在其后 100 字符内查找万元数字
        for sa in self.extract_service_areas(text):
            short = sa.replace("服务区", "")
            # 在文本中找到该 SA 的位置
            idx = text.find(short)
            if idx < 0:
                continue
            # 在该位置后 120 字内找数字
            window = text[idx:idx + 120]
            amounts = re.findall(r'([\d,]+\.?\d*)\s*万元', window)
            if amounts:
                nums = []
                for a in amounts:
                    try:
                        nums.append(float(a.replace(',', '')))
                    except ValueError:
                        pass
                if nums:
                    result[sa] = nums

        return result

    def detect_region_intent(self, question: str) -> Optional[str]:
        """
        从问题中检测片区意图
        返回片区全名（如 "皖北管理中心"）或 None
        """
        region_map = {
            "皖中": "皖中管理中心",
            "皖北": "皖北管理中心",
            "皖南": "皖南管理中心",
            "皖西": "皖西管理中心",
            "皖东": "皖东管理中心",
        }
        for short, full in region_map.items():
            if short in question:
                return full
        return None

    # ============================================================
    # 验证检查项
    # ============================================================

    def check_entity_existence(self, report: str) -> List[Issue]:
        """检查回答中提到的服务区是否在 DB 中存在"""
        issues = []

        # 通用短语排除（不是具体服务区名）
        generic_patterns = [
            "高速公路服务区", "进入服务区", "进服务区", "该服务区",
            "这些服务区", "各服务区", "头部服务区", "标杆服务区",
            "其他服务区", "某个服务区", "此服务区", "管辖的服务区",
            "表现突出的服务区", "最繁忙的服务区", "类高能级服务区",
            "标杆型头部服务区", "印证该服务区", "表明该服务区",
            "这三处服务区", "次于头部标杆服务区",
        ]

        # 全局正则提取 "XX服务区" 格式
        mentioned = re.findall(r'([\u4e00-\u9fa5]{2,8}服务区)', report)
        for sa_name in set(mentioned):
            # 跳过通用短语
            if any(gp in sa_name or sa_name in gp for gp in generic_patterns):
                continue

            if sa_name not in self._sa_names:
                # 可能是简写或别名，检查前缀
                short = sa_name.replace("服务区", "")
                possible = [s for s in self._sa_names if short in s]
                if not possible:
                    issues.append(Issue(
                        level="🟡",
                        category="entity",
                        message=f"未知服务区: {sa_name}",
                        detail={"sa_name": sa_name},
                    ))
        return issues

    def check_region_attribution(
        self, question: str, report: str,
        context_questions: List[str] = None,
    ) -> List[Issue]:
        """
        检查片区归属：问特定片区时，回答中的 SA 应属于该片区

        context_questions: 多轮场景中的前几轮问题（用于推断片区意图）
        """
        issues = []

        # 1. 从当前问题检测片区意图
        target_region = self.detect_region_intent(question)

        # 2. 如果当前问题没有片区信息，从上下文推断
        if not target_region and context_questions:
            for prev_q in reversed(context_questions):
                r = self.detect_region_intent(prev_q)
                if r:
                    target_region = r
                    break

        if not target_region:
            return issues

        # 3. 提取回答中的 SA，验证归属
        mentioned_sas = self.extract_service_areas(report)
        valid_sas = self._region_sas.get(target_region, set())

        for sa in mentioned_sas:
            actual_region = self._sa_region.get(sa)
            if actual_region and actual_region != target_region:
                # 过滤：如果 SA 只是在"对比"语境下提到（如"与XX对比"），不报错
                # 简单启发式：如果 SA 周围有"对比""比较""vs"不报
                idx = report.find(sa.replace("服务区", ""))
                if idx >= 0:
                    context_window = report[max(0, idx - 30):idx + 50]
                    skip_keywords = ["对比", "比较", "相比", "vs", "VS", "不同"]
                    if any(kw in context_window for kw in skip_keywords):
                        continue

                region_short = target_region[:2]
                msg = "片区归属错误: 问[" + region_short + "]->回答含[" + sa + "]->DB确认属于" + actual_region
                issues.append(Issue(
                    level="\U0001f534",
                    category="region",
                    message=msg,
                    detail={
                        "sa_name": sa,
                        "expected_region": target_region,
                        "actual_region": actual_region,
                        "hint": "检查工具调用是否传了正确的 region_id",
                    },
                ))

        return issues

    def _extract_month_from_question(self, question: str) -> str:
        """从问题中提取月份，返回 YYYYMM 格式"""
        import re as _re
        from datetime import datetime as _dt
        current_year = _dt.now().year
        current_month = _dt.now().month
        # "2月" "2月份" "二月" → YYYY02
        month_map = {
            "一": "01", "二": "02", "三": "03", "四": "04",
            "五": "05", "六": "06", "七": "07", "八": "08",
            "九": "09", "十": "10", "十一": "11", "十二": "12",
        }
        # 带年份的月份（"2025年3月"）
        ym = _re.search(r'(\d{4})\s*年\s*(\d{1,2})\s*月', question)
        if ym:
            return f"{ym.group(1)}{int(ym.group(2)):02d}"
        # 数字月份
        m = _re.search(r'(\d{1,2})\s*月', question)
        if m:
            month_num = int(m.group(1))
            if 1 <= month_num <= 12:
                return f"{current_year}{month_num:02d}"
        # 中文月份
        for cn, num in month_map.items():
            if cn + "月" in question:
                return f"{current_year}{num}"
        # "上个月" → 动态计算
        if "上个月" in question or "上月" in question:
            prev_month = current_month - 1 if current_month > 1 else 12
            prev_year = current_year if current_month > 1 else current_year - 1
            return f"{prev_year}{prev_month:02d}"
        return ""

    def check_revenue_numbers(
        self, question: str, report: str,
        context_questions: List[str] = None,
    ) -> List[Issue]:
        """营收数字核对：回答中的金额与镜像月度数据对比"""
        issues = []
        if not self._mirror_available:
            return issues

        # 1. 提取月份（从当前问题或上下文）
        target_month = self._extract_month_from_question(question)
        if not target_month and context_questions:
            for prev_q in reversed(context_questions):
                target_month = self._extract_month_from_question(prev_q)
                if target_month:
                    break

        # 2. 跳过日度/实时问题（日度数字与月度聚合差距太大）
        daily_keywords = ["昨天", "今天", "实时", "当前", "现在"]
        combined = question + (report[:100] if report else "")
        if any(kw in combined for kw in daily_keywords):
            return issues  # 日度验证需要 LOCAL_DAILY_REVENUE，后续实现

        # 3. 提取回答中的 SA+数字
        sa_numbers = self.extract_revenue_numbers(report)
        if not sa_numbers:
            return issues

        for sa, nums in sa_numbers.items():
            mirror_data = self._mirror_revenue.get(sa, {})
            if not mirror_data:
                # 尝试短名匹配
                short = sa.replace("服务区", "")
                for mkey in self._mirror_revenue:
                    if short in mkey:
                        mirror_data = self._mirror_revenue[mkey]
                        break

            if not mirror_data:
                continue  # 无镜像数据可比

            # 4. 选取同月份的镜像数据
            if target_month and target_month in mirror_data:
                mirror_val = mirror_data[target_month]  # 元
                mirror_wan = mirror_val / 10000  # 万元
            else:
                continue  # 无法确定月份，跳过（避免误报）

            # 5. 逐个数字对比
            for num in nums:
                # num 是万元单位（从 extract_revenue_numbers 提取）
                if mirror_wan > 0:
                    diff_pct = abs(num - mirror_wan) / mirror_wan * 100
                else:
                    continue

                # 容差 5%（同月同口径，误差不应太大）
                if diff_pct > 5:
                    # 区分严重程度
                    level = "🔴" if diff_pct > 50 else "🟡"
                    issues.append(Issue(
                        level=level,
                        category="revenue",
                        message=(
                            f"营收数字偏差: {sa} "
                            f"回答={num:.2f}万, "
                            f"镜像{target_month[-2:]}月={mirror_wan:.2f}万, "
                            f"偏差{diff_pct:.1f}%"
                        ),
                        detail={
                            "sa_name": sa,
                            "month": target_month,
                            "answer_value": num,
                            "mirror_value": round(mirror_wan, 2),
                            "diff_pct": round(diff_pct, 1),
                        },
                    ))

        return issues

    def check_ranking_order(
        self, question: str, report: str
    ) -> List[Issue]:
        """排名顺序验证：排名类回答的 SA 顺序与镜像排名比对"""
        issues = []
        if not self._ranking_data:
            return issues

        # 只在排名类问题中验证
        rank_keywords = ["排名", "排行", "前几", "前5", "前10", "前三", "前五", "最高", "最差", "垫底"]
        if not any(kw in question for kw in rank_keywords):
            return issues

        # 提取回答中提到的 SA 列表（按出现顺序）
        mentioned = self.extract_service_areas(report)
        if len(mentioned) < 2:
            return issues  # 至少2个SA才有排名比较意义

        # 构建镜像排名映射
        mirror_rank = {}
        for item in self._ranking_data:
            mirror_rank[item["name"]] = item["rank"]

        # 检查排名中是否有黑名单 SA
        for sa in mentioned:
            if sa in self._blacklist_names:
                issues.append(Issue(
                    level="🔴",
                    category="blacklist",
                    message=f"排名含非实体SA: {sa}（应被过滤）",
                    detail={"sa_name": sa},
                ))

        return issues

    def check_blacklist(
        self, question: str, report: str
    ) -> List[Issue]:
        """黑名单回归：回答不应包含非实体服务区"""
        issues = []
        if not self._blacklist_names:
            return issues

        mentioned = self.extract_service_areas(report)
        for sa in mentioned:
            if sa in self._blacklist_names:
                issues.append(Issue(
                    level="🟡",
                    category="blacklist",
                    message=f"回答含非实体SA: {sa}",
                    detail={"sa_name": sa},
                ))

        return issues

    def check_cross_turn_consistency(
        self, current_report: str,
        current_question: str = "",
        previous_reports: List[str] = None,
        context_questions: List[str] = None,
    ) -> List[Issue]:
        """跨轮一致性：同口径同时间下，同一 SA 的数字应一致"""
        issues = []
        if not previous_reports:
            return issues

        current_numbers = self.extract_revenue_numbers(current_report)
        if not current_numbers:
            return issues

        # 口径/时间关键词——如果前后轮包含不同关键词，说明口径变了
        dimension_keywords = [
            "昨天", "今天", "日", "实时",        # 日维度
            "利润", "盈利",                       # 利润口径
            "业主", "业主收入",                   # 业主口径
            "车流", "流量",                       # 车流口径
            "春节", "节假日", "春运",             # 节假日口径
            "坪效", "客单价",                     # 效率口径
        ]

        def _get_dimensions(text: str) -> set:
            return {kw for kw in dimension_keywords if kw in text}

        cur_dims = _get_dimensions(current_question)

        for idx, prev_report in enumerate(previous_reports):
            prev_q = context_questions[idx] if context_questions and idx < len(context_questions) else ""
            prev_dims = _get_dimensions(prev_q)

            # 口径/时间维度变了，跳过比较
            if cur_dims != prev_dims and (cur_dims or prev_dims):
                continue

            prev_numbers = self.extract_revenue_numbers(prev_report)
            for sa, cur_nums in current_numbers.items():
                prev_nums = prev_numbers.get(sa, [])
                if not prev_nums:
                    continue
                # 比较首个数字（主要金额）
                for cn in cur_nums[:1]:
                    for pn in prev_nums[:1]:
                        if cn > 0 and pn > 0:
                            diff_pct = abs(cn - pn) / max(cn, pn) * 100
                            if diff_pct > 5:  # 容差 5%
                                issues.append(Issue(
                                    level="🟡",
                                    category="consistency",
                                    message=(
                                        f"跨轮数字不一致: {sa} "
                                        f"当前={cn}万元 vs 之前={pn}万元 "
                                        f"(差{diff_pct:.1f}%)"
                                    ),
                                    detail={
                                        "sa_name": sa,
                                        "current": cn,
                                        "previous": pn,
                                        "diff_pct": round(diff_pct, 1),
                                    },
                                ))

        return issues

    # ============================================================
    # 主入口
    # ============================================================

    def verify(
        self,
        question: str,
        report: str,
        context_questions: List[str] = None,
        previous_reports: List[str] = None,
    ) -> L3Result:
        """
        对单条回答做 L3 深度验证

        Args:
            question: 当前问题
            report: AI 回答文本
            context_questions: 多轮场景中的前面轮次问题
            previous_reports: 多轮场景中的前面轮次回答
        """
        all_issues: List[Issue] = []

        # 1. 实体存在性
        all_issues += self.check_entity_existence(report)

        # 2. 片区归属
        all_issues += self.check_region_attribution(
            question, report, context_questions
        )

        # 3. 营收数字核对（按月同口径）
        all_issues += self.check_revenue_numbers(
            question, report, context_questions
        )

        # 4. 排名顺序验证
        all_issues += self.check_ranking_order(question, report)

        # 5. 黑名单回归
        all_issues += self.check_blacklist(question, report)

        # 6. 跨轮一致性
        all_issues += self.check_cross_turn_consistency(
            report, question, previous_reports, context_questions
        )

        # 汇总
        entities = self.extract_service_areas(report)
        numbers = self.extract_revenue_numbers(report)

        return L3Result(
            issues=all_issues,
            entities_found=entities,
            numbers_found={sa: nums[0] for sa, nums in numbers.items() if nums},
        )


# ============================================================
# 事后核对：对已有 JSON 结果文件做 L3 验证
# ============================================================

def verify_json_results(json_path: str, verifier: DeepVerifier) -> dict:
    """对 qa_runner 产生的 JSON 做事后 L3 核对"""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = {"scenarios": [], "total_issues": 0, "errors": 0, "warnings": 0}

    # 多轮结果
    if "scenarios" in data:
        for scenario in data["scenarios"]:
            scenario_result = {
                "name": scenario["name"],
                "turns": [],
                "issues_count": 0,
            }

            context_questions = []
            previous_reports = []

            for turn in scenario["turns"]:
                q = turn["question"]
                report = turn.get("report_preview", "")

                if not report:
                    context_questions.append(q)
                    previous_reports.append("")
                    continue

                l3 = verifier.verify(
                    question=q,
                    report=report,
                    context_questions=context_questions,
                    previous_reports=previous_reports,
                )

                turn_result = {
                    "turn": turn["turn"],
                    "question": q,
                    "entities": l3.entities_found,
                    "issues": [
                        {"level": i.level, "category": i.category,
                         "message": i.message, "detail": i.detail}
                        for i in l3.issues
                    ],
                }
                scenario_result["turns"].append(turn_result)
                scenario_result["issues_count"] += len(l3.issues)

                for i in l3.issues:
                    results["total_issues"] += 1
                    if i.level == "🔴":
                        results["errors"] += 1
                    elif i.level == "🟡":
                        results["warnings"] += 1

                context_questions.append(q)
                previous_reports.append(report)

            results["scenarios"].append(scenario_result)

    # 单轮结果
    elif "results" in data:
        for r in data["results"]:
            q = r["question"]
            report = r.get("report_preview", "")
            if not report:
                continue

            l3 = verifier.verify(question=q, report=report)
            for i in l3.issues:
                results["total_issues"] += 1
                if i.level == "🔴":
                    results["errors"] += 1
                elif i.level == "🟡":
                    results["warnings"] += 1

    return results


# ============================================================
# CLI 入口
# ============================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="QA 深度验证器（L3）")
    parser.add_argument(
        "--input", required=True,
        help="qa_runner 产生的 JSON 结果文件路径"
    )
    parser.add_argument(
        "--db", default=str(_MAIN_DB),
        help="主数据库路径（默认 d:/AISpace/AI-Python/data/db.sqlite3）"
    )
    parser.add_argument(
        "--mirror-db", default=str(_MIRROR_DB),
        help="镜像数据库路径（默认 d:/AISpace/AI-Python/data/dameng_mirror.db）"
    )
    args = parser.parse_args()

    print("🔍 QA 深度验证器 v1.0")
    print(f"   输入: {args.input}")

    verifier = DeepVerifier(args.db, args.mirror_db)
    results = verify_json_results(args.input, verifier)

    print(f"\n{'='*60}")
    print(f"📊 L3 验证结果")
    print(f"   总问题: {results['total_issues']}")
    print(f"   🔴 错误: {results['errors']}")
    print(f"   🟡 警告: {results['warnings']}")
    print(f"{'='*60}")

    # 打印各场景详情
    for scenario in results.get("scenarios", []):
        if scenario["issues_count"] == 0:
            continue
        print(f"\n📋 {scenario['name']} ({scenario['issues_count']} 个问题)")
        for turn in scenario["turns"]:
            if turn["issues"]:
                print(f"  轮{turn['turn']}: {turn['question']}")
                for issue in turn["issues"]:
                    print(f"    {issue['level']} [{issue['category']}] {issue['message']}")

    # 保存结果
    output_path = args.input.replace(".json", "_l3.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n💾 结果: {output_path}")


if __name__ == "__main__":
    main()

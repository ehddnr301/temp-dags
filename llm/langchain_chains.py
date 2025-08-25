import os
import json
from typing import Optional, Dict, Any, List

import pandas as pd

try:
    # LangChain OpenAI 챗 모델 (환경에 따라 provider가 다를 수 있음)
    from langchain_openai import ChatOpenAI
except Exception as exc:  # pragma: no cover - 환경별 의존성 차이를 완화
    ChatOpenAI = None  # type: ignore


def _ensure_dependencies() -> None:
    if ChatOpenAI is None:
        raise ImportError(
            "langchain_openai가 필요합니다. 설치: pip install langchain-openai langchain"
        )


def _get_llm(model: str = "gpt-5-mini", temperature: float = 0.2) -> Any:
    _ensure_dependencies()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY 환경변수가 설정되어야 합니다.")
    return ChatOpenAI(model=model, temperature=temperature)


def _dataframe_to_tsv(df: pd.DataFrame) -> str:
    required_columns = [
        "base_date",
        "organization",
        "repo_name",
        "source_event_type",
        "activity_text",
        "actor_login",
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"DataFrame에 필요한 컬럼이 없습니다: {missing}")

    ordered = (
        df.sort_values(["base_date", "organization", "repo_name", "actor_login"], ascending=True)
        .loc[:, required_columns]
        .astype(str)
    )
    header = "\t".join(required_columns)
    lines = [header]
    for _, row in ordered.iterrows():
        lines.append("\t".join(row[col] for col in required_columns))
    return "\n".join(lines)


def _parse_json_strict(text: str) -> Dict[str, Any]:
    """LLM 출력에서 JSON 블록만 안전하게 파싱합니다."""
    # 코드블록 안 JSON 추출 시도
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            part_stripped = part.strip()
            if part_stripped.startswith("{") and part_stripped.endswith("}"):
                return json.loads(part_stripped)
    # 일반 JSON 시도
    text_stripped = text.strip()
    start = text_stripped.find("{")
    end = text_stripped.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(text_stripped[start : end + 1])
    # 최종 실패
    return json.loads(text)


def _build_actor_week_prompt(week_start: str, week_end: str, tsv_block: str) -> str:
    return f"""
다음은 특정 주간(월요일~일요일)의 GitHub 활동 데이터입니다.
각 행은 [base_date, organization, repo_name, source_event_type, activity_text, actor_login]을 의미합니다.
입력은 단일 actor_login에 해당하는 한 주간 묶음입니다.
요청사항:

1) 한 주간의 핵심 활동 요약을 2~4문장으로 작성
2) 눈에 띄는 변화(예: 릴리스 버전 증가, 문서화 집중, Star 유입 등)를 1~3개 bullet로 추출
3) 다음 주 행동 제안(선택): 1~2개 (있을 때만)

출력은 아래 JSON 스키마를 엄격히 따르세요.

JSON 스키마:
{{
  "week_start": "YYYY-MM-DD",
  "actor_login": "string",
  "organization_set": ["string", ...],
  "top_repos": [{{"repo_name": "string", "events": ["PushEvent", "CreateEvent", "..."]}}],
  "summary": "2~4문장",
  "highlights": ["핵심 포인트 1", "핵심 포인트 2"],
  "suggestions": ["제안 1"]
}}

데이터(탭 구분 TSV):
{tsv_block}

반드시 유효한 JSON만 반환하고, 불필요한 설명/코드블록 마커는 포함하지 마세요.
다음 필드는 필수로 채우세요: week_start={week_start}, week_end={week_end}.
""".strip()


def _build_repo_week_prompt(week_start: str, week_end: str, tsv_block: str) -> str:
    return f"""
다음은 특정 주간(월요일~일요일)의 GitHub 활동 데이터입니다.
각 행은 [base_date, organization, repo_name, source_event_type, activity_text, actor_login]을 의미합니다.
입력은 단일 repo_name에 해당하는 한 주간 묶음입니다.
요청사항:

1) 레포의 주간 핵심 변경사항 2~4문장
2) 기여자 상위(최대 3명): actor_login 및 대표 활동(요약)
3) 릴리스/태그/브랜치 생성 등 “관리 이벤트”와 “개발 활동(커밋/PR)”을 구분하여 하이라이트

JSON 스키마:
{{
  "week_start": "YYYY-MM-DD",
  "organization": "string",
  "repo_name": "string",
  "contributors_top": [{{"actor_login": "string", "notable_activity": "string"}}],
  "summary": "2~4문장",
  "highlights_dev": ["개발 활동 포인트 1", "…"],
  "highlights_manage": ["관리 이벤트 포인트 1", "…"],
  "community_signals": ["Star/Watch 등 신호 요약"],
  "risks_or_followups": ["잠재 리스크/후속 과제"]
}}

데이터(탭 구분 TSV):
{tsv_block}

반드시 유효한 JSON만 반환하고, 불필요한 설명/코드블록 마커는 포함하지 마세요.
다음 필드는 필수로 채우세요: week_start={week_start}, week_end={week_end}.
""".strip()


def generate_actor_week_report(
    df: pd.DataFrame,
    week_start: str,
    week_end: str,
    actor_login: str,
    model: str = "gpt-5-mini",
    temperature: float = 0.2,
) -> Dict[str, Any]:
    """단일 액터의 주간 활동 요약 JSON을 생성합니다.

    df: `llm/main.py` 쿼리 결과 DataFrame
    week_start: YYYY-MM-DD (월요일)
    actor_login: 단일 액터 ID
    """
    if "week_start" not in df.columns:
        raise ValueError("DataFrame에 'week_start' 컬럼이 필요합니다.")

    df_actor = df[(df["week_start"].astype(str) == week_start) & (df["actor_login"] == actor_login)]
    if df_actor.empty:
        raise ValueError("해당 조건에 맞는 행이 없습니다.")

    tsv = _dataframe_to_tsv(df_actor)
    prompt = _build_actor_week_prompt(week_start=week_start, week_end=week_end, tsv_block=tsv)
    llm = _get_llm(model=model, temperature=temperature)
    resp = llm.invoke(prompt)
    return _parse_json_strict(resp.content if hasattr(resp, "content") else str(resp))


def generate_repo_week_report(
    df: pd.DataFrame,
    week_start: str,
    week_end: str,
    organization: str,
    repo_name: str,
    model: str = "gpt-5-mini",
    temperature: float = 0.2,
) -> Dict[str, Any]:
    """단일 레포의 주간 활동 요약 JSON을 생성합니다.

    df: `llm/main.py` 쿼리 결과 DataFrame
    week_start: YYYY-MM-DD (월요일)
    organization: 조직명
    repo_name: 레포 이름
    """
    if "week_start" not in df.columns:
        raise ValueError("DataFrame에 'week_start' 컬럼이 필요합니다.")

    df_repo = df[
        (df["week_start"].astype(str) == week_start)
        & (df["organization"] == organization)
        & (df["repo_name"] == repo_name)
    ]
    if df_repo.empty:
        raise ValueError("해당 조건에 맞는 행이 없습니다.")

    tsv = _dataframe_to_tsv(df_repo)
    prompt = _build_repo_week_prompt(week_start=week_start, week_end=week_end, tsv_block=tsv)
    llm = _get_llm(model=model, temperature=temperature)
    resp = llm.invoke(prompt)
    return _parse_json_strict(resp.content if hasattr(resp, "content") else str(resp))


def generate_actor_week_reports_batch(
    df: pd.DataFrame,
    week_start: str,
    week_end: str,
    model: str = "gpt-5-mini",
    temperature: float = 0.2,
) -> List[Dict[str, Any]]:
    """해당 주차의 모든 actor_login에 대해 actor-week 요약을 배치 생성합니다."""
    if "week_start" not in df.columns:
        raise ValueError("DataFrame에 'week_start' 컬럼이 필요합니다.")
    df_week = df[df["week_start"].astype(str) == week_start]
    if df_week.empty:
        return []
    actors = (
        df_week["actor_login"].dropna().astype(str).unique().tolist()
    )
    results: List[Dict[str, Any]] = []
    for actor in actors:
        try:
            results.append(
                generate_actor_week_report(
                    df=df_week,
                    week_start=week_start,
                    week_end=week_end,
                    actor_login=actor,
                    model=model,
                    temperature=temperature,
                )
            )
        except Exception as exc:
            # 개별 실패는 스킵하고 계속 진행
            continue
    return results


def generate_repo_week_reports_batch(
    df: pd.DataFrame,
    week_start: str,
    week_end: str,
    model: str = "gpt-5-mini",
    temperature: float = 0.2,
) -> List[Dict[str, Any]]:
    """해당 주차의 모든 (organization, repo_name) 조합에 대해 repo-week 요약을 배치 생성합니다."""
    if "week_start" not in df.columns:
        raise ValueError("DataFrame에 'week_start' 컬럼이 필요합니다.")
    df_week = df[df["week_start"].astype(str) == week_start]
    if df_week.empty:
        return []
    pairs = (
        df_week[["organization", "repo_name"]]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    results: List[Dict[str, Any]] = []
    for organization, repo_name in pairs:
        try:
            results.append(
                generate_repo_week_report(
                    df=df_week,
                    week_start=week_start,
                    week_end=week_end,
                    organization=organization,
                    repo_name=repo_name,
                    model=model,
                    temperature=temperature,
                )
            )
        except Exception as exc:
            # 개별 실패는 스킵하고 계속 진행
            continue
    return results


"""
사용 예시:

from llm.main import pd  # 이미 pandas를 사용 중
from llm.langchain_chains import (
    generate_actor_week_report,
    generate_repo_week_report,
)

# df = main.py에서 생성된 DataFrame (week_start 포함)
# actor 리포트
# actor_json = generate_actor_week_report(df, week_start="2025-03-03", actor_login="octocat")

# repo 리포트
# repo_json = generate_repo_week_report(df, week_start="2025-03-03", organization="my-org", repo_name="my-repo")
"""



import streamlit as st
import altair as alt
import json
import pandas as pd
import streamlit.components.v1 as components
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="MoveRadar v2", layout="wide", page_icon="📡")

session = get_active_session()

# ── 데이터 로드 ─────────────────────────────────────────────────

def load_alerts():
    return session.sql("""
        SELECT
            SGG, REGION_KEY, DISTRICT_CODE,
            ALERT_DATE::VARCHAR          AS ALERT_DATE,
            ALERT_TYPE,
            ROUND(COMBINED_SCORE, 3)     AS COMBINED_SCORE,
            ROUND(PRICE_SCORE, 3)        AS PRICE_SCORE,
            ROUND(POP_SCORE, 3)          AS POP_SCORE,
            ROUND(TELECOM_SCORE, 3)      AS TELECOM_SCORE,
            ROUND(CARD_SCORE, 3)         AS CARD_SCORE,
            DOMINANT_AGE_GROUP,
            INCOME_PROFILE,
            TRENDING_RENTALS,
            MARKETING_COPY
        FROM MOVERADAR.PUBLIC.MARKETING_ALERTS
        ORDER BY COMBINED_SCORE DESC
    """).to_pandas()

def load_sgg_summary():
    return session.sql("""
        SELECT * FROM MOVERADAR.PUBLIC.SGG_SUMMARY
        ORDER BY TOTAL_ALERTS DESC
    """).to_pandas()

def load_signal_ts(region_key):
    return session.sql(f"""
        SELECT SIGNAL_TYPE, TS, ACTUAL_VALUE, FORECAST_VALUE, PERCENTILE, IS_ANOMALY
        FROM MOVERADAR.PUBLIC.SIGNAL_TIMESERIES
        WHERE REGION_KEY = '{region_key}'
        ORDER BY TS
    """).to_pandas()

def load_telecom_trend(sgg):
    return session.sql(f"""
        SELECT YEAR_MONTH::VARCHAR AS YM, MAIN_CATEGORY_NAME, OPENS, CONTRACTS
        FROM MOVERADAR.PUBLIC.TELECOM_REGIONAL_TREND
        WHERE SGG = '{sgg}'
        ORDER BY YM
    """).to_pandas()

def load_rental_context():
    return session.sql("""
        SELECT RENTAL_MAIN_CATEGORY, RENTAL_SUB_CATEGORY,
               TOTAL_OPEN, ROUND(AVG_CVR_PCT, 1) AS CVR_PCT, RANK_NUM
        FROM MOVERADAR.PUBLIC.RENTAL_TREND_CONTEXT
        ORDER BY RANK_NUM
        LIMIT 10
    """).to_pandas()

def load_call_center():
    return session.sql("""
        SELECT YEAR_MONTH::VARCHAR AS YM, MAIN_CATEGORY_NAME,
               TOTAL_CALLS, ROUND(AVG_CONNECTION_RATE, 1) AS CONN_RATE,
               ROUND(CALL_TO_CONTRACT_CVR, 2) AS CALL_CVR,
               ROUND(AVG_LEADTIME_DAYS, 1) AS LEAD_DAYS
        FROM MOVERADAR.PUBLIC.CALL_CENTER_CONTEXT
        ORDER BY YM DESC
        LIMIT 30
    """).to_pandas()

# 로드
alerts = load_alerts()

if alerts.empty:
    st.error("MARKETING_ALERTS 데이터가 없습니다. pipeline.sql STEP 6c를 먼저 실행해 주세요.")
    st.stop()

alerts["REGION"] = alerts["SGG"]
alerts["YM"] = alerts["ALERT_DATE"].str[:7]

# ── 경보 유형별 색상 ────────────────────────────────────────────
ALERT_COLORS = {
    "4중 동시 경보":            "#4a148c",
    "3중 동시 경보":            "#7b1fa2",
    "시세+전입인구+카드 경보":  "#ad1457",
    "시세+통신+카드 경보":      "#c62828",
    "전입인구+통신+카드 경보":  "#0d47a1",
    "시세+전입인구 경보":        "#d32f2f",
    "시세+통신 경보":            "#e64a19",
    "시세+카드 경보":            "#bf360c",
    "전입인구+통신 경보":        "#1565c0",
    "전입인구+카드 경보":        "#1976d2",
    "통신+카드 경보":            "#00695c",
    "시세 경보":                "#f57c00",
    "전입인구 경보":             "#1976d2",
    "통신 경보":                "#388e3c",
    "카드소비 경보":             "#6a1b9a",
}

# ── 헤더 ────────────────────────────────────────────────────────
st.title("📡 MoveRadar v2 — 이사 수요 경보 대시보드")
st.caption("신호 4개: 아파트 시세 · 전입인구 · 통신 개통 · 카드소비(가전/가구) 이상 탐지 | Snowflake Cortex ML")

# ── 사이드바 필터 ────────────────────────────────────────────────
with st.sidebar:
    st.header("필터")
    sgg_opts = ["전체"] + sorted(alerts["SGG"].unique().tolist())
    sel_sgg = st.selectbox("구 선택", sgg_opts)
    type_opts = ["전체"] + sorted(alerts["ALERT_TYPE"].dropna().unique().tolist())
    sel_type = st.selectbox("경보 유형", type_opts)
    score_min = st.slider("최소 경보 점수", 0.0, 1.0, 0.0, 0.05)

filtered = alerts.copy()
if sel_sgg != "전체":
    filtered = filtered[filtered["SGG"] == sel_sgg]
if sel_type != "전체":
    filtered = filtered[filtered["ALERT_TYPE"] == sel_type]
filtered = filtered[filtered["COMBINED_SCORE"] >= score_min].reset_index(drop=True)

# ── 탭 ──────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["🔔 경보 현황", "📈 지역 신호 추이", "📱 통신 분석", "📣 마케팅 문구"])


# ════════════════════════════════════════════════════════════════
# TAB 1: 경보 현황
# ════════════════════════════════════════════════════════════════
with tab1:
    # KPI
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("총 경보", len(filtered))
    k2.metric("4중 동시 경보", int((filtered["ALERT_TYPE"] == "4중 동시 경보").sum()))
    k3.metric("최고 점수", f"{filtered['COMBINED_SCORE'].max():.3f}" if not filtered.empty else "-")
    k4.metric("대상 구", filtered["SGG"].nunique())
    k5.metric("3중 이상 경보", int(filtered["ALERT_TYPE"].str.contains("3중|4중", na=False).sum()))

    st.divider()

    col_chart, col_detail = st.columns([3, 2])

    with col_chart:
        st.subheader("지역별 경보 점수 TOP 25")

        chart_data = (
            filtered.groupby(["REGION", "ALERT_TYPE", "SGG"], as_index=False)
            ["COMBINED_SCORE"].max()
            .sort_values("COMBINED_SCORE", ascending=False)
            .head(25)
        )

        if not chart_data.empty:
            bar = (
                alt.Chart(chart_data)
                .mark_bar()
                .encode(
                    x=alt.X("COMBINED_SCORE:Q", scale=alt.Scale(domain=[0, 1]), title="결합 경보 점수"),
                    y=alt.Y("REGION:N", sort="-x", title="지역"),
                    color=alt.Color(
                        "ALERT_TYPE:N",
                        scale=alt.Scale(
                            domain=list(ALERT_COLORS.keys()),
                            range=list(ALERT_COLORS.values()),
                        ),
                        legend=alt.Legend(title="경보 유형"),
                    ),
                    tooltip=["REGION", "ALERT_TYPE", alt.Tooltip("COMBINED_SCORE:Q", format=".3f")],
                )
                .properties(height=520)
            )
            st.altair_chart(bar, width='stretch')

        # 신호별 점수 분포 (4개 신호)
        st.subheader("4개 신호 점수 분포")
        score_melt = filtered[["REGION", "PRICE_SCORE", "POP_SCORE", "TELECOM_SCORE", "CARD_SCORE"]].melt(
            id_vars="REGION", var_name="신호", value_name="점수"
        )
        score_melt["신호"] = score_melt["신호"].map({
            "PRICE_SCORE": "시세", "POP_SCORE": "전입인구",
            "TELECOM_SCORE": "통신", "CARD_SCORE": "카드소비"
        })
        violin = (
            alt.Chart(score_melt)
            .mark_boxplot(extent="min-max")
            .encode(
                x=alt.X("신호:N", title=""),
                y=alt.Y("점수:Q", scale=alt.Scale(domain=[0, 1])),
                color=alt.Color("신호:N", scale=alt.Scale(
                    domain=["시세", "전입인구", "통신", "카드소비"],
                    range=["#f57c00", "#1976d2", "#388e3c", "#6a1b9a"],
                )),
            )
            .properties(height=220)
        )
        st.altair_chart(violin, width='stretch')

    with col_detail:
        st.subheader("지역 상세 & 마케팅 문구")

        if filtered.empty:
            st.info("선택 조건에 해당하는 경보가 없습니다.")
        else:
            label_list = [
                f"{r['REGION']}  {r['YM']}  ({r['COMBINED_SCORE']:.3f})  [{r['ALERT_TYPE']}]"
                for _, r in filtered.iterrows()
            ]
            sel_idx = st.selectbox(
                "지역/날짜 선택",
                range(len(label_list)),
                format_func=lambda i: label_list[i],
            )
            row = filtered.iloc[sel_idx]

            st.divider()
            m1, m2 = st.columns(2)
            m1.metric("결합 점수", f"{row['COMBINED_SCORE']:.3f}")
            m2.metric("경보 유형", row["ALERT_TYPE"] or "-")

            m3, m4, m5, m6 = st.columns(4)
            m3.metric("시세", f"{row['PRICE_SCORE']:.3f}")
            m4.metric("전입인구", f"{row['POP_SCORE']:.3f}")
            m5.metric("통신", f"{row['TELECOM_SCORE']:.3f}")
            m6.metric("카드소비", f"{row['CARD_SCORE']:.3f}")

            st.write(f"**주요 고객층:** {row['DOMINANT_AGE_GROUP'] or '-'}")
            st.write(f"**소득 수준:** {row['INCOME_PROFILE'] or '-'}")
            st.write(f"**인기 렌탈 상품:** {row['TRENDING_RENTALS'] or '-'}")

            st.divider()
            st.subheader("LLM 마케팅 문구")
            alert_color = ALERT_COLORS.get(row["ALERT_TYPE"], "#607d8b")
            st.markdown(
                f"<div style='background:{alert_color}20;border-left:4px solid {alert_color};"
                f"padding:12px;border-radius:4px;font-size:16px;'>"
                f"{row['MARKETING_COPY'] or '문구 없음'}</div>",
                unsafe_allow_html=True,
            )

    # 전체 경보 목록
    with st.expander("📋 전체 경보 목록"):
        disp = filtered[[
            "REGION", "YM", "ALERT_TYPE", "COMBINED_SCORE",
            "PRICE_SCORE", "POP_SCORE", "TELECOM_SCORE", "CARD_SCORE",
            "DOMINANT_AGE_GROUP", "INCOME_PROFILE"
        ]].copy()
        disp.columns = ["지역", "기준월", "경보유형", "결합점수", "시세", "전입인구", "통신", "카드소비", "주요연령대", "소득수준"]
        st.dataframe(disp, width='stretch', hide_index=True)


# ════════════════════════════════════════════════════════════════
# TAB 2: 지역 신호 추이
# ════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("지역별 4개 신호 시계열 추이")
    st.caption("Cortex Anomaly Detection: 실제값 vs 예측값, PERCENTILE 기준 이상 구간 표시")

    all_regions = sorted(alerts["REGION_KEY"].unique().tolist())
    sel_region = st.selectbox("분석할 지역 선택", all_regions,
                               format_func=lambda k: k.replace("_", " "))

    if sel_region:
        ts_df = load_signal_ts(sel_region)
        if ts_df.empty:
            st.info("해당 지역의 신호 데이터가 없습니다.")
        else:
            ts_df["TS"] = pd.to_datetime(ts_df["TS"])

            signal_labels = {
                "price":   "🏠 시세",
                "pop":     "👥 전입인구",
                "telecom": "📱 통신 개통",
                "card":    "💳 카드소비(가전·가구)",
            }
            signal_colors = {
                "price":   "#f57c00",
                "pop":     "#1976d2",
                "telecom": "#388e3c",
                "card":    "#6a1b9a",
            }

            for sig_key, sig_label in signal_labels.items():
                sig_df = ts_df[ts_df["SIGNAL_TYPE"] == sig_key].copy()
                if sig_df.empty:
                    continue

                st.markdown(f"#### {sig_label}")

                base = alt.Chart(sig_df).encode(x=alt.X("TS:T", title="날짜"))

                actual_line = base.mark_line(
                    color=signal_colors[sig_key], strokeWidth=2
                ).encode(y=alt.Y("ACTUAL_VALUE:Q", title="실제값"))

                forecast_line = base.mark_line(
                    color="#9e9e9e", strokeDash=[4, 2]
                ).encode(y=alt.Y("FORECAST_VALUE:Q"))

                pct_area = base.mark_area(
                    opacity=0.15, color=signal_colors[sig_key]
                ).encode(
                    y=alt.Y("PERCENTILE:Q", scale=alt.Scale(domain=[0, 1]), title="백분위"),
                    y2=alt.value(0),
                )

                threshold = alt.Chart(
                    pd.DataFrame({"y": [0.75, 0.25]})
                ).mark_rule(color="red", strokeDash=[3, 3], opacity=0.5).encode(y="y:Q")

                chart = alt.layer(
                    actual_line, forecast_line
                ).resolve_scale(y="independent").properties(height=200)

                pct_chart = alt.layer(
                    pct_area, threshold
                ).properties(height=100)

                combined = alt.vconcat(chart, pct_chart).resolve_scale(x="shared")
                st.altair_chart(combined, width='stretch')

    # SGG 요약 히트맵
    st.divider()
    st.subheader("구별 경보 현황 요약")
    try:
        sgg_df = load_sgg_summary()
        if not sgg_df.empty:
            heat_data = sgg_df[["SGG", "PRICE_ALERTS", "POP_ALERTS", "TELECOM_ALERTS", "CARD_ALERTS"]].melt(
                id_vars="SGG", var_name="신호", value_name="경보수"
            )
            heat_data["신호"] = heat_data["신호"].map({
                "PRICE_ALERTS": "시세", "POP_ALERTS": "전입인구",
                "TELECOM_ALERTS": "통신", "CARD_ALERTS": "카드소비"
            })
            heatmap = (
                alt.Chart(heat_data)
                .mark_rect()
                .encode(
                    x=alt.X("신호:N", title="신호"),
                    y=alt.Y("SGG:N", sort=alt.EncodingSortField("경보수", op="sum", order="descending"), title="구"),
                    color=alt.Color("경보수:Q", scale=alt.Scale(scheme="orangered")),
                    tooltip=["SGG", "신호", "경보수"],
                )
                .properties(height=500, title="구별 신호별 경보 건수 히트맵")
            )
            st.altair_chart(heatmap, width='stretch')
    except Exception as e:
        st.warning(f"SGG 요약 로드 실패: {e}")


# ════════════════════════════════════════════════════════════════
# TAB 3: 통신 분석
# ════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("📱 아정네트웍스 통신 데이터 분석")

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown("#### 🔥 인기 렌탈 상품 TOP 10 (최근 3개월 서울)")
        try:
            rental_df = load_rental_context()
            if not rental_df.empty:
                rental_chart = (
                    alt.Chart(rental_df)
                    .mark_bar(color="#7b1fa2")
                    .encode(
                        x=alt.X("TOTAL_OPEN:Q", title="개통 건수"),
                        y=alt.Y("RENTAL_MAIN_CATEGORY:N", sort="-x", title="렌탈 대분류"),
                        tooltip=["RENTAL_MAIN_CATEGORY", "RENTAL_SUB_CATEGORY", "TOTAL_OPEN", "CVR_PCT"],
                    )
                    .properties(height=300)
                )
                st.altair_chart(rental_chart, width='stretch')
                st.dataframe(rental_df, width='stretch', hide_index=True)
        except Exception as e:
            st.warning(f"렌탈 트렌드 로드 실패 (STEP 6a 미실행?): {e}")

    with col_right:
        st.markdown("#### 📞 콜센터 월별 트렌드")
        try:
            call_df = load_call_center()
            if not call_df.empty:
                call_chart = (
                    alt.Chart(call_df)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("YM:N", title="월"),
                        y=alt.Y("TOTAL_CALLS:Q", title="통화 건수"),
                        color=alt.Color("MAIN_CATEGORY_NAME:N", legend=alt.Legend(title="상품")),
                        tooltip=["YM", "MAIN_CATEGORY_NAME", "TOTAL_CALLS", "CONN_RATE", "CALL_CVR"],
                    )
                    .properties(height=200)
                )
                st.altair_chart(call_chart, width='stretch')

                st.markdown("**콜→계약 전환율 & 리드타임**")
                cvr_chart = (
                    alt.Chart(call_df.dropna(subset=["CALL_CVR"]))
                    .mark_bar()
                    .encode(
                        x=alt.X("YM:N", title="월"),
                        y=alt.Y("CALL_CVR:Q", title="전환율(%)"),
                        color="MAIN_CATEGORY_NAME:N",
                    )
                    .properties(height=150)
                )
                st.altair_chart(cvr_chart, width='stretch')
        except Exception as e:
            st.warning(f"콜센터 데이터 로드 실패: {e}")

    st.divider()
    st.markdown("#### 📍 구별 통신 개통 트렌드")
    tc_sgg = st.selectbox("구 선택 (통신)", sorted(alerts["SGG"].unique().tolist()), key="tc_sgg")
    try:
        tc_df = load_telecom_trend(tc_sgg)
        if not tc_df.empty:
            tc_chart = (
                alt.Chart(tc_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("YM:N", title="월"),
                    y=alt.Y("OPENS:Q", title="개통 건수"),
                    color=alt.Color("MAIN_CATEGORY_NAME:N", legend=alt.Legend(title="상품")),
                    tooltip=["YM", "MAIN_CATEGORY_NAME", "OPENS", "CONTRACTS"],
                )
                .properties(height=280, title=f"{tc_sgg} 월별 통신 개통 현황")
            )
            st.altair_chart(tc_chart, width='stretch')
        else:
            st.info("해당 구의 통신 데이터가 없습니다.")
    except Exception as e:
        st.warning(f"통신 트렌드 로드 실패: {e}")


# ════════════════════════════════════════════════════════════════
# TAB 4: 마케팅 문구 라이브러리
# ════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("📣 LLM 마케팅 문구 라이브러리")
    st.caption("Snowflake Cortex COMPLETE (mistral-large2) 생성 문구 | 15가지 경보 유형별 맞춤 작성")

    # 경보 유형별 탭
    alert_types_present = [t for t in ALERT_COLORS.keys()
                           if t in alerts["ALERT_TYPE"].values]

    if not alert_types_present:
        st.info("마케팅 문구 데이터가 없습니다.")
    else:
        type_tabs = st.tabs(alert_types_present)
        for i, atype in enumerate(alert_types_present):
            with type_tabs[i]:
                subset = alerts[alerts["ALERT_TYPE"] == atype].sort_values(
                    "COMBINED_SCORE", ascending=False
                )
                color = ALERT_COLORS.get(atype, "#607d8b")

                for _, row in subset.iterrows():
                    with st.container():
                        c1, c2 = st.columns([3, 1])
                        with c1:
                            st.markdown(
                                f"<div style='background:{color}15;border-left:4px solid {color};"
                                f"padding:10px 14px;border-radius:4px;margin-bottom:8px;'>"
                                f"<b>{row['REGION']} ({row['YM']})</b><br/>"
                                f"<span style='font-size:15px;'>{row['MARKETING_COPY'] or '문구 없음'}</span>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )
                        with c2:
                            st.metric("결합 점수", f"{row['COMBINED_SCORE']:.3f}")
                            st.caption(f"{row['DOMINANT_AGE_GROUP'] or '-'}")
                            st.caption(f"{row['INCOME_PROFILE'] or '-'}")

    st.divider()

    # 경보 유형별 평균 점수 비교
    st.markdown("#### 경보 유형별 평균 점수")
    if not alerts.empty:
        type_summary = (
            alerts.groupby("ALERT_TYPE")[
                ["COMBINED_SCORE", "PRICE_SCORE", "POP_SCORE", "TELECOM_SCORE", "CARD_SCORE"]
            ]
            .mean()
            .round(3)
            .reset_index()
        )
        type_melt = type_summary.melt(
            id_vars="ALERT_TYPE", var_name="신호", value_name="평균 점수"
        )
        type_melt["신호"] = type_melt["신호"].map({
            "COMBINED_SCORE": "결합",
            "PRICE_SCORE": "시세",
            "POP_SCORE": "전입인구",
            "TELECOM_SCORE": "통신",
            "CARD_SCORE": "카드소비",
        })
        type_chart = (
            alt.Chart(type_melt)
            .mark_bar()
            .encode(
                x=alt.X("평균 점수:Q", scale=alt.Scale(domain=[0, 1])),
                y=alt.Y("ALERT_TYPE:N", sort="-x"),
                color=alt.Color("신호:N", scale=alt.Scale(
                    domain=["결합", "시세", "전입인구", "통신", "카드소비"],
                    range=["#424242", "#f57c00", "#1976d2", "#388e3c", "#6a1b9a"],
                )),
                xOffset="신호:N",
                tooltip=["ALERT_TYPE", "신호", alt.Tooltip("평균 점수:Q", format=".3f")],
            )
            .properties(height=400)
        )
        st.altair_chart(type_chart, width='stretch')

"""
Gera o dashboard analítico (reports/dashboard.html) a partir da camada Gold.

Lê exclusivamente da camada Gold (+ Silver para o recorte "Aluno", que precisa
dos microdados individuais, não agregados na Gold) e produz agregações prontas
para consumo direto pelo front-end — nenhum dado bruto de aluno é embutido no
HTML, só os agregados. O JSON resultante é injetado em
reports/dashboard_template.html, substituindo o placeholder
__DASHBOARD_DATA_JSON__, gerando reports/dashboard.html — um único arquivo
HTML autocontido, sem dependências externas, que abre em qualquer navegador.

Uso:
    python reports/gerar_dashboard_dados.py
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from pipeline.batch.config import GOLD_DIR, SILVER_DIR
from quality.validacao_dados import executar_validacao_completa

PONTO_CORTE_SAEB = 743
MIN_ALUNOS_UF = 300  # limiar mínimo de alunos avaliados para entrar no ranking por UF (evita ruído de amostra pequena)


def _ler(camada_dir: Path, nome: str) -> pd.DataFrame:
    return pq.ParquetDataset(str(camada_dir / nome)).read().to_pandas()


def _num(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    return round(float(x), 2)


# ─── Brasil ────────────────────────────────────────────────────────────────────

def montar_brasil(evolucao: pd.DataFrame, meta_brasil: pd.DataFrame) -> dict:
    br = evolucao[evolucao["nivel"] == "brasil"].sort_values("ano")
    serie = [
        {"ano": int(r.ano), "indicador": _num(r.indicador), "variacao_yoy": _num(r.variacao_yoy)}
        for r in br.itertuples()
    ]

    meta_2030 = meta_brasil[meta_brasil["ano_meta"] == 2030]
    meta_2030_valor = _num(meta_2030["valor_meta"].iloc[0]) if len(meta_2030) else None

    trajetoria_meta = meta_brasil[meta_brasil["ano_meta"] >= 2024].sort_values("ano_meta")
    serie_meta = [
        {"ano": int(r.ano_meta), "meta": _num(r.valor_meta)}
        for r in trajetoria_meta.itertuples()
    ]

    ultimo = serie[-1] if serie else None
    primeiro = serie[0] if serie else None

    return {
        "serie": serie,
        "serie_meta": serie_meta,
        "headline": {
            "ano_atual": ultimo["ano"] if ultimo else None,
            "indicador_atual": ultimo["indicador"] if ultimo else None,
            "variacao_yoy": ultimo["variacao_yoy"] if ultimo else None,
            "meta_2030": meta_2030_valor,
            "gap_meta_2030": _num(meta_2030_valor - ultimo["indicador"]) if (meta_2030_valor and ultimo) else None,
            "delta_periodo": _num(ultimo["indicador"] - primeiro["indicador"]) if (ultimo and primeiro) else None,
            "ano_inicial": primeiro["ano"] if primeiro else None,
        },
    }


# ─── UF ────────────────────────────────────────────────────────────────────────

def montar_uf(evolucao: pd.DataFrame, comparacao: pd.DataFrame) -> dict:
    uf = evolucao[evolucao["nivel"] == "uf"].copy()
    anos = sorted(uf["ano"].unique().tolist())
    ano_min, ano_max = anos[0], anos[-1]

    series_por_uf = {}
    for sigla, grupo in uf.groupby("referencia", observed=True):
        grupo = grupo.sort_values("ano")
        series_por_uf[sigla] = [{"ano": int(r.ano), "indicador": _num(r.indicador)} for r in grupo.itertuples()]

    pivot = uf.pivot_table(index="referencia", columns="ano", values="indicador", observed=True)
    pivot["delta"] = pivot[ano_max] - pivot[ano_min]

    comp_atual = comparacao[comparacao["ano"] == comparacao["ano"].max()].set_index("sigla_uf")

    def _linha(sigla, row):
        info = comp_atual.loc[sigla] if sigla in comp_atual.index else None
        return {
            "sigla_uf": sigla,
            "indicador_atual": _num(row[ano_max]),
            "meta_alfabetizacao": _num(info["meta_alfabetizacao"]) if info is not None else None,
            "gap": _num(info["gap"]) if info is not None else None,
            "atingiu_meta": bool(info["atingiu_meta"]) if info is not None else None,
            "pct_municipios_meta_atingida": _num(info["pct_municipios_meta_atingida"]) if info is not None else None,
            "delta": _num(row["delta"]),
        }

    linhas = [_linha(sigla, row) for sigla, row in pivot.iterrows()]

    ranking_atual = sorted([l for l in linhas if l["indicador_atual"] is not None], key=lambda l: l["indicador_atual"])
    ranking_delta = sorted([l for l in linhas if l["delta"] is not None], key=lambda l: l["delta"])

    return {
        "anos": anos,
        "series_por_uf": series_por_uf,
        "ranking": linhas,
        "melhores": list(reversed(ranking_atual[-5:])),
        "piores": ranking_atual[:5],
        "mais_melhoraram": list(reversed(ranking_delta[-5:])),
        "mais_pioraram": ranking_delta[:5],
    }


# ─── Município ─────────────────────────────────────────────────────────────────

def montar_municipio(muni: pd.DataFrame) -> dict:
    anos = sorted(muni["ano"].unique().tolist())
    ano_min, ano_max = anos[0], anos[-1]

    atual = muni[muni["ano"] == ano_max].copy()

    def _linha(r):
        return {
            "id_municipio": r.id_municipio,
            "nome_municipio": r.nome_municipio,
            "sigla_uf": r.sigla_uf,
            "nome_regiao": r.nome_regiao,
            "taxa_alfabetizacao": _num(r.taxa_alfabetizacao),
            "faixa_risco": r.faixa_risco,
            "idhm": _num(r.idhm) if pd.notna(r.idhm) else None,
        }

    ranking_atual = sorted(
        [_linha(r) for r in atual.itertuples() if pd.notna(r.taxa_alfabetizacao)],
        key=lambda l: l["taxa_alfabetizacao"],
    )

    pivot = muni.pivot_table(index="id_municipio", columns="ano", values="taxa_alfabetizacao", observed=True)
    info_muni = atual.set_index("id_municipio")[["nome_municipio", "sigla_uf", "nome_regiao"]]
    comparaveis = pivot.dropna(subset=[ano_min, ano_max]).copy()
    comparaveis["delta"] = comparaveis[ano_max] - comparaveis[ano_min]
    comparaveis = comparaveis.join(info_muni, how="left")

    def _linha_delta(idx, row):
        return {
            "id_municipio": idx,
            "nome_municipio": row["nome_municipio"],
            "sigla_uf": row["sigla_uf"],
            "nome_regiao": row["nome_regiao"],
            "delta": _num(row["delta"]),
            "taxa_inicial": _num(row[ano_min]),
            "taxa_atual": _num(row[ano_max]),
        }

    linhas_delta = [_linha_delta(idx, row) for idx, row in comparaveis.iterrows()]
    ranking_delta = sorted(linhas_delta, key=lambda l: l["delta"])

    # Municípios que atingiram a própria meta municipal (só existe comparação
    # para quem tem rede Municipal avaliada E meta municipal definida no ano)
    com_meta = int(atual["atingiu_meta_municipal"].notna().sum())
    atingiram_meta = int((atual["atingiu_meta_municipal"] == True).sum())  # noqa: E712
    pct_atingiram_meta = _num(atingiram_meta / com_meta * 100) if com_meta else None

    return {
        "anos": anos,
        "total_municipios_avaliados": int(atual["id_municipio"].nunique()),
        "meta_municipal": {
            "ano": int(ano_max),
            "total_com_meta": com_meta,
            "atingiram": atingiram_meta,
            "pct_atingiram": pct_atingiram_meta,
        },
        "melhores": list(reversed(ranking_atual[-10:])),
        "piores": ranking_atual[:10],
        "mais_melhoraram": list(reversed(ranking_delta[-10:])),
        "mais_pioraram": ranking_delta[:10],
    }


# ─── Aluno (microdados — Silver) ───────────────────────────────────────────────

def montar_aluno(alunos: pd.DataFrame, diretorio_municipio: pd.DataFrame) -> dict:
    df = alunos[alunos["presenca"] == True].copy()  # noqa: E712 — só alunos efetivamente avaliados
    df["ano"] = df["ano"].astype(int)
    df["alfabetizado_bin"] = df["alfabetizado"].astype("boolean")

    anos = sorted(df["ano"].unique().tolist())
    ano_min, ano_max = anos[0], anos[-1]

    # ─ Histórico Brasil (todas as redes) ─
    historico = []
    for ano, grupo in df.groupby("ano"):
        prof = grupo["proficiencia"].dropna()
        historico.append({
            "ano": int(ano),
            "total_avaliados": int(len(grupo)),
            "pct_alfabetizado": _num(grupo["alfabetizado_bin"].mean() * 100),
            "proficiencia_media": _num(prof.mean()),
        })
    historico = sorted(historico, key=lambda h: h["ano"])

    # ─ Distribuição de proficiência (ano mais recente) ─
    prof_atual = df[df["ano"] == ano_max]["proficiencia"].dropna()
    bins = np.arange(560, 921, 20)
    contagem, bordas = np.histogram(prof_atual, bins=bins)
    distribuicao = [
        {"faixa_min": int(bordas[i]), "faixa_max": int(bordas[i + 1]), "contagem": int(contagem[i])}
        for i in range(len(contagem))
    ]

    # ─ Por rede de ensino (ano mais recente) ─
    por_rede = []
    for rede, grupo in df[df["ano"] == ano_max].groupby("rede_label", observed=True):
        prof = grupo["proficiencia"].dropna()
        por_rede.append({
            "rede": rede,
            "total_avaliados": int(len(grupo)),
            "pct_alfabetizado": _num(grupo["alfabetizado_bin"].mean() * 100),
            "proficiencia_media": _num(prof.mean()),
        })
    por_rede = sorted(por_rede, key=lambda r: -r["total_avaliados"])

    # ─ Por UF (join com diretorio_municipio para obter sigla_uf) ─
    dir_muni = diretorio_municipio[["id_municipio", "sigla_uf"]].drop_duplicates()
    df_uf = df.merge(dir_muni, on="id_municipio", how="left")

    agg_uf = (
        df_uf.groupby(["ano", "sigla_uf"], observed=True)
        .agg(
            total_avaliados=("id_aluno", "count"),
            pct_alfabetizado=("alfabetizado_bin", "mean"),
            proficiencia_media=("proficiencia", "mean"),
        )
        .reset_index()
    )
    agg_uf["pct_alfabetizado"] = agg_uf["pct_alfabetizado"] * 100

    atual_uf = agg_uf[(agg_uf["ano"] == ano_max) & (agg_uf["total_avaliados"] >= MIN_ALUNOS_UF)]
    ranking_uf_atual = sorted(
        [
            {
                "sigla_uf": r.sigla_uf,
                "total_avaliados": int(r.total_avaliados),
                "pct_alfabetizado": _num(r.pct_alfabetizado),
                "proficiencia_media": _num(r.proficiencia_media),
            }
            for r in atual_uf.itertuples()
        ],
        key=lambda l: l["proficiencia_media"],
    )

    pivot_uf = agg_uf.pivot_table(index="sigla_uf", columns="ano", values="proficiencia_media")
    pivot_n = agg_uf.pivot_table(index="sigla_uf", columns="ano", values="total_avaliados")
    comparaveis_uf = pivot_uf.dropna(subset=[ano_min, ano_max]).copy()
    comparaveis_uf = comparaveis_uf[
        (pivot_n.loc[comparaveis_uf.index, ano_min] >= MIN_ALUNOS_UF)
        & (pivot_n.loc[comparaveis_uf.index, ano_max] >= MIN_ALUNOS_UF)
    ]
    comparaveis_uf["delta"] = comparaveis_uf[ano_max] - comparaveis_uf[ano_min]

    linhas_delta_uf = [
        {
            "sigla_uf": idx,
            "proficiencia_inicial": _num(row[ano_min]),
            "proficiencia_atual": _num(row[ano_max]),
            "delta": _num(row["delta"]),
        }
        for idx, row in comparaveis_uf.iterrows()
    ]
    ranking_delta_uf = sorted(linhas_delta_uf, key=lambda l: l["delta"])

    return {
        "ponto_corte_saeb": PONTO_CORTE_SAEB,
        "historico": historico,
        "distribuicao_proficiencia": distribuicao,
        "por_rede": por_rede,
        "melhores_uf": list(reversed(ranking_uf_atual[-5:])),
        "piores_uf": ranking_uf_atual[:5],
        "mais_melhoraram_uf": list(reversed(ranking_delta_uf[-5:])),
        "mais_pioraram_uf": ranking_delta_uf[:5],
    }


# ─── Qualidade de dados ────────────────────────────────────────────────────────

def montar_qualidade() -> dict:
    """Roda a validação real (quality/validacao_dados.py) e resume por camada."""
    resultado = executar_validacao_completa()

    resumo_camadas = {}
    total = {"tabelas_verificadas": 0, "tabelas_ok": 0, "tabelas_com_alerta": 0, "total_alertas": 0}

    for camada in ("bronze", "silver", "gold"):
        tabelas = resultado[camada]
        verificadas = {k: v for k, v in tabelas.items() if v["status"] != "nao_encontrada"}
        ok = sum(1 for v in verificadas.values() if v["status"] == "OK")
        com_alerta = sum(1 for v in verificadas.values() if v["status"] == "ALERTA")
        n_alertas = sum(len(v.get("alertas", [])) for v in verificadas.values())

        resumo_camadas[camada] = {
            "tabelas_verificadas": len(verificadas),
            "tabelas_ok": ok,
            "tabelas_com_alerta": com_alerta,
            "total_alertas": n_alertas,
        }
        total["tabelas_verificadas"] += len(verificadas)
        total["tabelas_ok"] += ok
        total["tabelas_com_alerta"] += com_alerta
        total["total_alertas"] += n_alertas

    return {
        "timestamp": resultado["timestamp"],
        "camadas": resumo_camadas,
        "total": total,
    }


def main():
    evolucao = _ler(GOLD_DIR, "evolucao_temporal")
    comparacao = _ler(GOLD_DIR, "comparacao_metas_resultados")
    muni = _ler(GOLD_DIR, "indicador_alfabetizacao_municipio")
    meta_brasil = _ler(SILVER_DIR, "meta_brasil")
    alunos = _ler(SILVER_DIR, "alunos")
    diretorio_municipio = _ler(SILVER_DIR, "diretorio_municipio")

    # Colunas de partição voltam como Categorical (não-ordenado) — precisa de
    # int puro para comparação/ordenação numérica abaixo.
    comparacao["ano"] = comparacao["ano"].astype(int)
    muni["ano"] = muni["ano"].astype(int)
    meta_brasil["ano_meta"] = meta_brasil["ano_meta"].astype(int)

    dados = {
        "gerado_em": pd.Timestamp.utcnow().isoformat(),
        "brasil": montar_brasil(evolucao, meta_brasil),
        "uf": montar_uf(evolucao, comparacao),
        "municipio": montar_municipio(muni),
        "aluno": montar_aluno(alunos, diretorio_municipio),
        "qualidade": montar_qualidade(),
    }

    dados_json = json.dumps(dados, ensure_ascii=False, indent=2)

    destino_json = ROOT_DIR / "reports" / "dashboard_data.json"
    destino_json.write_text(dados_json, encoding="utf-8")
    print(f"Salvo: {destino_json} ({destino_json.stat().st_size / 1024:.1f} KB)")

    template_path = ROOT_DIR / "reports" / "dashboard_template.html"
    template = template_path.read_text(encoding="utf-8")
    if "__DASHBOARD_DATA_JSON__" not in template:
        raise RuntimeError(f"Placeholder __DASHBOARD_DATA_JSON__ não encontrado em {template_path}")

    # Evita que "</script>" dentro do JSON (não deveria ocorrer, mas por segurança) quebre o parsing do HTML
    dados_json_seguro = dados_json.replace("</script", "<\\/script")
    html_final = template.replace("__DASHBOARD_DATA_JSON__", dados_json_seguro)

    destino_html = ROOT_DIR / "reports" / "dashboard.html"
    destino_html.write_text(html_final, encoding="utf-8")
    print(f"Salvo: {destino_html} ({destino_html.stat().st_size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Observatório Econômico de Jandira — Pipeline de Dados
Busca CAGED (MTE), IBGE SIDRA e atualiza os JSONs do dashboard.

Fontes:
  CAGED: http://pdet.mte.gov.br/novo-caged  (mensal, ~25 dias após competência)
  IBGE:  https://servicodados.ibge.gov.br/api/v3/  (anual/decenal)
"""

import io, json, logging, os, re, sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuração ──────────────────────────────────────────────────────────────
MUNICIPIO_CODE = 3525003        # Jandira SP — código IBGE 7 dígitos
MUNICIPIO_STR  = "3525003"
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# Grau de instrução → rótulo
INSTRUCAO = {
    1: "Analfabeto", 2: "Fund. Incompleto", 3: "Fund. Completo",
    4: "Médio Incompleto", 5: "Médio Completo", 6: "Sup. Incompleto",
    7: "Sup. Completo", 8: "Mestrado", 9: "Doutorado", -1: "Ignorado"
}

# Seção CNAE → rótulo
SECAO_CNAE = {
    "A": "Agropecuária", "B": "Mineração", "C": "Indústria Transformação",
    "D": "Eletricidade/Gás", "E": "Água/Saneamento", "F": "Construção Civil",
    "G": "Comércio", "H": "Transporte/Armazenagem", "I": "Alimentação/Hospedagem",
    "J": "TI/Comunicação", "K": "Financeiro/Seguros", "L": "Imobiliário",
    "M": "Serv. Profissionais", "N": "Serv. Administrativos", "O": "Admin. Pública",
    "P": "Educação", "Q": "Saúde/Assistência Social", "R": "Arte/Cultura",
    "S": "Outros Serviços", "T": "Serv. Domésticos", "U": "Org. Internacionais"
}

# Divisão CNAE C (Indústria de Transformação) → rótulo
DIV_IND = {
    "10": "Alimentos", "11": "Bebidas", "12": "Fumo",
    "13": "Têxteis", "14": "Vestuário", "15": "Couro/Calçados",
    "16": "Madeira", "17": "Celulose/Papel", "18": "Gráfica",
    "19": "Coque/Petróleo", "20": "Químicos", "21": "Farmacêuticos",
    "22": "Borracha/Plásticos", "23": "Minerais Não-Metálicos",
    "24": "Metalurgia", "25": "Metal/Máquinas", "26": "Eletrônicos",
    "27": "Máq. Elétricas", "28": "Máq. Mecânicas", "29": "Veículos/Autos",
    "30": "Outros Veículos", "31": "Móveis", "32": "Diversos", "33": "Manutenção"
}

# Subclasse C23 (Minerais Não-Metálicos) → rótulo
SUB_C23 = {
    "2310500": "Cimento", "2320600": "Cal virgem e cal hidratada",
    "2330301": "Cerâmica para construção (tijolo, telha)",
    "2330302": "Artefatos de concreto/fibrocimento",
    "2330303": "Artefatos de gesso",
    "2391503": "Aparelhamento de pedras",
    "2399101": "Abrasivos", "2399199": "Outros minerais não-metálicos"
}


# ── Funções auxiliares ────────────────────────────────────────────────────────

def save_json(name: str, data: dict):
    path = DATA_DIR / f"{name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"Salvo: {path}")


def load_json(name: str, default=None):
    path = DATA_DIR / f"{name}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default or {}


# ── CAGED: detecção da competência mais recente ──────────────────────────────

def latest_caged_candidates():
    """Gera (ano, mês) a tentar, do mais recente para trás."""
    now = datetime.now()
    # CAGED sai ~20-25 dias após o mês de competência.
    # Tentamos mês atual -1, -2 e -3 para garantir.
    for delta_months in range(1, 7):
        d = now - timedelta(days=30 * delta_months)
        yield d.year, d.month


def build_caged_urls(year: int, month: int):
    """Retorna lista de URLs para tentar (diferentes padrões do MTE)."""
    ym = f"{year}{month:02d}"
    return [
        # Padrão 1 — portal BI MTE
        f"https://bi.mte.gov.br/bgcaged/caged_ftp/public/md_caged_mov_{ym}.7z",
        # Padrão 2 — PDET/MTE direto
        f"http://pdet.mte.gov.br/assets/novo-caged/{year}-{month}/1-CAGEDMOV{ym}.zip",
        # Padrão 3 — ftp via http proxy
        f"https://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{year}/{ym}/CAGEDMOV{ym}.7z",
    ]


def try_download(url: str, timeout: int = 120) -> bytes | None:
    """Tenta baixar a URL; retorna bytes ou None em caso de erro."""
    try:
        log.info(f"Tentando: {url}")
        r = requests.get(url, timeout=timeout, stream=True,
                         headers={"User-Agent": "ObservatorioJandira/1.0"})
        if r.status_code == 200:
            buf = bytearray()
            for chunk in r.iter_content(65536):
                buf.extend(chunk)
            log.info(f"Download OK: {len(buf)/1e6:.1f} MB")
            return bytes(buf)
        log.warning(f"HTTP {r.status_code}: {url}")
    except Exception as e:
        log.warning(f"Erro ao baixar {url}: {e}")
    return None


def extract_csv_from_archive(data: bytes, year: int, month: int):
    """Extrai o CSV do arquivo .7z ou .zip e retorna como texto."""
    ym = f"{year}{month:02d}"

    # Tenta .7z primeiro
    if data[:6] == b"7z\xbc\xaf'\x1c":
        try:
            import py7zr
            with py7zr.SevenZipFile(io.BytesIO(data)) as zf:
                names = zf.getnames()
                log.info(f"Arquivos no .7z: {names}")
                # Extrai o primeiro CSV encontrado
                for name in names:
                    if name.lower().endswith(".csv") or name.lower().endswith(".txt"):
                        extracted = zf.read([name])
                        return list(extracted.values())[0].read().decode("latin-1")
        except Exception as e:
            log.warning(f"Erro ao ler .7z: {e}")

    # Tenta .zip
    if data[:2] == b"PK":
        try:
            import zipfile
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                names = zf.namelist()
                log.info(f"Arquivos no .zip: {names}")
                for name in names:
                    if name.lower().endswith(".csv") or name.lower().endswith(".txt"):
                        return zf.read(name).decode("latin-1")
        except Exception as e:
            log.warning(f"Erro ao ler .zip: {e}")

    return None


def parse_caged_csv(csv_text: str):
    """
    Lê o CSV do CAGED em chunks, filtra Jandira e agrega os dados.
    Retorna dict com todas as agregações necessárias para o dashboard.
    """
    try:
        import pandas as pd
    except ImportError:
        log.error("pandas não instalado. Execute: pip install pandas")
        return None

    log.info("Processando CSV do CAGED (filtrando Jandira)...")

    # Lê em chunks para economizar memória (arquivo pode ser > 1 GB)
    buf = io.StringIO(csv_text)
    chunks = []
    try:
        for chunk in pd.read_csv(
            buf, sep=";", encoding="latin-1", chunksize=200_000,
            dtype={"Município": str, "Subclasse": str},
            low_memory=False
        ):
            # Normaliza nome das colunas
            chunk.columns = [c.strip() for c in chunk.columns]

            # Detecta coluna de município (pode variar levemente)
            mun_col = next(
                (c for c in chunk.columns if "munic" in c.lower()), None
            )
            if mun_col is None:
                log.error(f"Coluna de município não encontrada. Colunas: {list(chunk.columns)}")
                return None

            # Filtra Jandira
            sub = chunk[chunk[mun_col].astype(str).str.strip() == MUNICIPIO_STR]
            if len(sub) > 0:
                chunks.append(sub)

        if not chunks:
            log.warning("Nenhum registro encontrado para Jandira neste arquivo.")
            return None

        df = pd.concat(chunks, ignore_index=True)
        log.info(f"Registros de Jandira: {len(df)}")

    except Exception as e:
        log.error(f"Erro ao ler CSV: {e}")
        return None

    # ── Detectar colunas ──────────────────────────────────────────────────────
    def find_col(keywords):
        for kw in keywords:
            for c in df.columns:
                if kw.lower() in c.lower():
                    return c
        return None

    col_tipo   = find_col(["tipo de mov", "tipodemov", "tipomov"])
    col_secao  = find_col(["seção", "secao", "seção "])
    col_sub    = find_col(["subclasse"])
    col_sal    = find_col(["salário mensal", "salariomensal", "salario mensal"])
    col_instr  = find_col(["grau de inst", "grauinst", "instrucao"])
    col_sexo   = find_col(["sexo"])
    col_idade  = find_col(["idade"])

    log.info(f"Colunas mapeadas: tipo={col_tipo}, seção={col_secao}, sub={col_sub}, "
             f"sal={col_sal}, instr={col_instr}, sexo={col_sexo}, idade={col_idade}")

    # ── Admissões e desligamentos ─────────────────────────────────────────────
    admissoes = 0
    desligamentos = 0
    if col_tipo:
        tipo = df[col_tipo].astype(str).str.strip()
        # Tipos: 10=adm, 20=des, 25=transf.entrada, 35=transf.saída, 40=morte, 50=reintegração
        admissoes    = int((tipo.isin(["10", "25", "50"])).sum())
        desligamentos = int((tipo.isin(["20", "35", "40"])).sum())

    saldo = admissoes - desligamentos

    # ── Por seção CNAE ────────────────────────────────────────────────────────
    by_section = []
    if col_secao and col_tipo:
        for sec, grp in df.groupby(col_secao):
            sec = str(sec).strip()
            tipo = grp[col_tipo].astype(str).str.strip()
            adm = int((tipo.isin(["10", "25", "50"])).sum())
            des = int((tipo.isin(["20", "35", "40"])).sum())
            entry = {
                "section": sec,
                "name": SECAO_CNAE.get(sec, sec),
                "admissions": adm,
                "dismissals": des,
                "balance": adm - des,
                "details": []
            }

            # Drill-down por subclasse (primeiros 2 dígitos = divisão CNAE)
            if col_sub:
                for subcode, subgrp in grp.groupby(col_sub):
                    subcode = str(subcode).strip()
                    div = subcode[:2] if len(subcode) >= 2 else subcode
                    s_tipo = subgrp[col_tipo].astype(str).str.strip()
                    s_adm = int((s_tipo.isin(["10", "25", "50"])).sum())
                    s_des = int((s_tipo.isin(["20", "35", "40"])).sum())
                    # Rótulo
                    if sec == "C":
                        sub_name = DIV_IND.get(div, f"Div. {div}")
                        # Se é C23, tenta subclasse específica
                        if div == "23":
                            sub_name = SUB_C23.get(subcode, sub_name)
                    else:
                        sub_name = f"Subclasse {subcode}"

                    entry["details"].append({
                        "code": subcode,
                        "name": sub_name,
                        "admissions": s_adm,
                        "dismissals": s_des,
                        "balance": s_adm - s_des
                    })

            # Agrupa divisões dentro de C (limita a top 10)
            if sec == "C" and entry["details"]:
                div_map = {}
                for d in entry["details"]:
                    div = d["code"][:2]
                    name = DIV_IND.get(div, f"Div. {div}")
                    if div not in div_map:
                        div_map[div] = {"code": div, "name": name,
                                        "admissions": 0, "dismissals": 0,
                                        "balance": 0, "subclasses": []}
                    div_map[div]["admissions"] += d["admissions"]
                    div_map[div]["dismissals"] += d["dismissals"]
                    div_map[div]["balance"]    += d["balance"]
                    div_map[div]["subclasses"].append(d)
                entry["divisions"] = sorted(
                    div_map.values(), key=lambda x: abs(x["balance"]), reverse=True
                )[:12]

            by_section.append(entry)

        by_section.sort(key=lambda x: abs(x["balance"]), reverse=True)

    # ── Por escolaridade ──────────────────────────────────────────────────────
    by_education = []
    if col_instr and col_tipo:
        tipo = df[col_tipo].astype(str).str.strip()
        for inst_code, grp in df.groupby(col_instr):
            try:
                inst_int = int(str(inst_code).strip())
            except Exception:
                inst_int = -1
            s_tipo = grp[col_tipo].astype(str).str.strip()
            adm = int((s_tipo.isin(["10", "25", "50"])).sum())
            des = int((s_tipo.isin(["20", "35", "40"])).sum())
            by_education.append({
                "code": inst_int,
                "name": INSTRUCAO.get(inst_int, f"Nível {inst_int}"),
                "admissions": adm,
                "dismissals": des,
                "balance": adm - des
            })
        by_education.sort(key=lambda x: x["code"])

    # ── Por faixa salarial (em SM) ────────────────────────────────────────────
    by_salary = []
    if col_sal:
        try:
            df["_sal"] = pd.to_numeric(df[col_sal], errors="coerce")
            bins = [0, 1, 2, 3, 5, 10, float("inf")]
            labels = ["Até 1 SM", "1–2 SM", "2–3 SM", "3–5 SM", "5–10 SM", "Acima 10 SM"]
            df["_faixa"] = pd.cut(df["_sal"], bins=bins, labels=labels, right=True)
            for faixa, grp in df.groupby("_faixa"):
                if col_tipo:
                    s_tipo = grp[col_tipo].astype(str).str.strip()
                    adm = int((s_tipo.isin(["10", "25", "50"])).sum())
                    des = int((s_tipo.isin(["20", "35", "40"])).sum())
                else:
                    adm = len(grp)
                    des = 0
                by_salary.append({"faixa": str(faixa), "admissions": adm, "dismissals": des})
        except Exception as e:
            log.warning(f"Erro na faixa salarial: {e}")

    # ── Por sexo e idade ──────────────────────────────────────────────────────
    gender = {"M": 0, "F": 0, "other": 0}
    if col_sexo:
        sexo = df[col_sexo].astype(str).str.strip()
        gender["M"] = int((sexo == "1").sum())
        gender["F"] = int((sexo == "3").sum())
        gender["other"] = int(len(df) - gender["M"] - gender["F"])

    age_dist = []
    if col_idade:
        try:
            df["_idade"] = pd.to_numeric(df[col_idade], errors="coerce")
            faixas = [(18, 24), (25, 34), (35, 44), (45, 54), (55, 64), (65, 120)]
            for low, high in faixas:
                n = int(((df["_idade"] >= low) & (df["_idade"] <= high)).sum())
                age_dist.append({"range": f"{low}–{high}", "count": n})
        except Exception as e:
            log.warning(f"Erro na distribuição de idade: {e}")

    return {
        "admissions": admissoes,
        "dismissals": desligamentos,
        "balance": saldo,
        "total_records": len(df),
        "by_section": by_section,
        "by_education": by_education,
        "by_salary": by_salary,
        "gender": gender,
        "age_dist": age_dist
    }


# ── IBGE SIDRA ────────────────────────────────────────────────────────────────

def fetch_sidra(table_id: int, var_id: int, period: str = "last 1") -> dict:
    url = (f"https://servicodados.ibge.gov.br/api/v3/agregados/{table_id}"
           f"/periodos/{period}/variaveis/{var_id}"
           f"?localidades=N6[{MUNICIPIO_STR}]&formato=JSON")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        series = data[0]["resultados"][0]["series"]
        if series:
            return {k: v for k, v in series[0]["serie"].items()}
    except Exception as e:
        log.warning(f"SIDRA t={table_id} v={var_id}: {e}")
    return {}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Observatório Econômico de Jandira — Pipeline de dados ===")
    now = datetime.utcnow().isoformat() + "Z"

    meta = load_json("metadata", {
        "last_run": now,
        "sources": {}
    })
    meta["last_run"] = now

    # ── 1. IBGE SIDRA ──────────────────────────────────────────────────────
    log.info("Buscando dados IBGE SIDRA...")
    sidra_pop  = fetch_sidra(9514, 93)   # população estimada
    sidra_pib  = fetch_sidra(5938, 37)   # PIB per capita (tabela 5938 var 37)

    ibge_data = {
        "populacao": sidra_pop,
        "pib_percapita": sidra_pib,
        "fetched_at": now
    }
    save_json("ibge_sidra", ibge_data)
    meta["sources"]["ibge_sidra"] = {
        "label": "IBGE SIDRA",
        "fetched_at": now,
        "status": "ok" if sidra_pop else "sem dados",
        "description": "População estimada e PIB per capita"
    }

    # ── 2. CAGED ───────────────────────────────────────────────────────────
    log.info("Buscando dados do Novo CAGED (MTE)...")
    caged_result = None
    caged_year = None
    caged_month = None

    for year, month in latest_caged_candidates():
        urls = build_caged_urls(year, month)
        for url in urls:
            raw = try_download(url)
            if raw is None:
                continue
            csv_text = extract_csv_from_archive(raw, year, month)
            if csv_text is None:
                log.warning("Arquivo baixado mas não foi possível extrair CSV.")
                continue
            result = parse_caged_csv(csv_text)
            if result:
                caged_result = result
                caged_year = year
                caged_month = month
                break
        if caged_result:
            break

    if caged_result:
        caged_out = {
            "period": f"{caged_year}-{caged_month:02d}",
            "period_label": (
                ["Jan","Fev","Mar","Abr","Mai","Jun",
                 "Jul","Ago","Set","Out","Nov","Dez"][caged_month - 1]
                + f"/{caged_year}"
            ),
            "fetched_at": now,
            **caged_result
        }
        save_json("caged_latest", caged_out)
        log.info(f"CAGED {caged_year}-{caged_month:02d} salvo. "
                 f"Adm={caged_result['admissions']} Des={caged_result['dismissals']} "
                 f"Saldo={caged_result['balance']}")

        meta["sources"]["caged"] = {
            "label": "Novo CAGED (MTE)",
            "period": f"{caged_year}-{caged_month:02d}",
            "period_label": caged_out["period_label"],
            "fetched_at": now,
            "status": "ok",
            "description": "Admissões, desligamentos e saldo por setor CNAE"
        }
    else:
        log.warning("Não foi possível baixar CAGED. Mantendo dados anteriores.")
        meta["sources"]["caged"] = {
            "label": "Novo CAGED (MTE)",
            "fetched_at": now,
            "status": "falha — mantendo dados anteriores",
            "description": "Download falhou nesta execução"
        }

    # ── Salva metadados ────────────────────────────────────────────────────
    save_json("metadata", meta)
    log.info("=== Pipeline concluído ===")


if __name__ == "__main__":
    main()

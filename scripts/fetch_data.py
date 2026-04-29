#!/usr/bin/env python3
"""
Observatorio Economico de Jandira - Pipeline de Dados
Busca CAGED (MTE) e atualiza os JSONs do dashboard.

Fontes:
  CAGED: portal MTE/PDET (mensal, liberado ~20-25 dias apos competencia)
  IBGE:  servicodados.ibge.gov.br (populacao, PIB)
"""

import io, json, logging, os, sys, zipfile
from datetime import datetime, timedelta
from pathlib import Path

# Suprime avisos de SSL (necessario para contornar problemas nos servidores MTE)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Configuracao ─────────────────────────────────────────────────────────────
MUNICIPIO_CODE = 3525003
MUNICIPIO_STR  = "3525003"
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

MESES_PT = ["Jan","Fev","Mar","Abr","Mai","Jun",
            "Jul","Ago","Set","Out","Nov","Dez"]

# ─── Secoes CNAE ──────────────────────────────────────────────────────────────
SECAO_LABEL = {
    "A": "Agropecuaria", "B": "Mineracao",
    "C": "Industria de Transformacao",
    "D": "Eletricidade e Gas", "E": "Agua e Saneamento",
    "F": "Construcao Civil",
    "G": "Comercio e Reparacao",
    "H": "Transporte e Logistica",
    "I": "Alimentacao e Hospedagem",
    "J": "TI e Comunicacao",
    "K": "Financeiro e Seguros",
    "L": "Imobiliario",
    "M": "Servicos Profissionais",
    "N": "Servicos Administrativos",
    "O": "Administracao Publica",
    "P": "Educacao",
    "Q": "Saude e Servicos Sociais",
    "R": "Arte e Cultura",
    "S": "Outros Servicos",
    "T": "Servicos Domesticos",
}

# ─── Divisoes por secao (2 primeiros digitos da subclasse) ────────────────────
DIV = {
    "C": {
        "10": "Alimentos e Bebidas", "11": "Bebidas (excl. alcoolicas)",
        "12": "Fumo", "13": "Texteis", "14": "Vestuario",
        "15": "Couro e Calcados", "16": "Madeira",
        "17": "Celulose e Papel", "18": "Grafica",
        "19": "Coque e Derivados de Petroleo",
        "20": "Quimicos", "21": "Farmaceuticos",
        "22": "Borracha e Plasticos",
        "23": "Minerais Nao-Metalicos",
        "24": "Metalurgia", "25": "Produtos de Metal",
        "26": "Eletronicos", "27": "Maquinas Eletricas",
        "28": "Maquinas e Equipamentos",
        "29": "Veiculos e Automoveis",
        "30": "Outros Veiculos", "31": "Moveis",
        "32": "Diversos", "33": "Manutencao e Instalacao",
    },
    "G": {
        "45": "Veiculos Automotores",
        "46": "Comercio Atacadista",
        "47": "Comercio Varejista",
    },
    "H": {
        "49": "Transporte Terrestre",
        "50": "Transporte Aquaviario",
        "51": "Transporte Aereo",
        "52": "Armazenagem e Apoio",
        "53": "Correios e Entrega",
    },
    "F": {
        "41": "Edificacoes Residenciais e Comerciais",
        "42": "Obras de Infraestrutura",
        "43": "Servicos Especializados de Construcao",
    },
    "N": {
        "77": "Locacao de Bens Moveis",
        "78": "Selecao, Agenciamento e Locacao de MO",
        "79": "Agencias de Viagem e Turismo",
        "80": "Seguranca e Vigilancia",
        "81": "Servicos para Predios e Paisagismo",
        "82": "Servicos de Escritorio e Apoio",
    },
    "Q": {
        "86": "Atividades de Saude",
        "87": "Residencias Terapeuticas",
        "88": "Servicos Sociais sem Internacao",
    },
    "I": {
        "55": "Alojamento",
        "56": "Alimentacao",
    },
    "M": {
        "69": "Juridico e Contabilidade",
        "70": "Gestao Empresarial",
        "71": "Arquitetura e Engenharia",
        "72": "Pesquisa e Desenvolvimento",
        "73": "Publicidade e Pesquisa de Mercado",
        "74": "Design, Fotografia e Traducao",
        "75": "Veterinaria",
    },
    "J": {
        "58": "Edicao", "59": "Audio e Video",
        "60": "Radio e TV", "61": "Telecomunicacoes",
        "62": "TI e Software", "63": "Dados e Portais Web",
    },
    "P": {
        "85": "Educacao",
    },
    "S": {
        "94": "Associacoes e Sindicatos",
        "95": "Reparacao de Eletronicos",
        "96": "Servicos Pessoais",
    },
}

# ─── Subclasses por divisao-chave (secao+2digits) ─────────────────────────────
SUB = {
    "C23": {
        "2310500": "Cimento",
        "2320600": "Cal Virgem e Cal Hidratada",
        "2330301": "Ceramica para Construcao (tijolo, telha)",
        "2330302": "Artefatos de Concreto e Fibrocimento",
        "2330303": "Artefatos de Gesso",
        "2341900": "Artigos de Ceramica",
        "2349401": "Revestimentos de Ceramica",
        "2392300": "Cal e Gesso (outros)",
    },
    "C22": {
        "2211100": "Pneumaticos e Camaras de Ar",
        "2212900": "Recapagem de Pneumaticos",
        "2219600": "Outros Artefatos de Borracha",
        "2221800": "Embalagens Plasticas",
        "2222600": "Tubos e Conexoes Plasticas",
        "2229300": "Outros Artefatos de Plastico",
    },
    "C25": {
        "2511000": "Estruturas Metalicas",
        "2512800": "Esquadrias Metalicas",
        "2513600": "Caldeiraria Pesada",
        "2539001": "Ferramentas Manuais",
        "2543800": "Cutelaria",
        "2599399": "Outros Produtos de Metal",
    },
    "C10": {
        "1011201": "Frigorifico - Abate Bovino",
        "1012101": "Frigorifico - Abate Suino",
        "1031700": "Fabricacao de Conservas de Frutas",
        "1041400": "Fabricacao de Oleos Brutos",
        "1051100": "Laticinios",
        "1061901": "Beneficiamento de Arroz",
        "1066000": "Moagem de Trigo",
        "1091101": "Fabricacao de Biscoitos e Bolachas",
        "1092900": "Fabricacao de Chocolates",
        "1099699": "Outros Alimentos",
    },
    "G47": {
        "4711302": "Hipermercados e Supermercados",
        "4712100": "Minimercados, Mercearias e Armazens",
        "4721102": "Padarias e Confeitarias",
        "4729699": "Outros Comercios de Alimentos",
        "4741500": "Material de Construcao",
        "4744001": "Ferragens e Ferramentas",
        "4751200": "Eletrodomesticos e Eletronicos",
        "4771701": "Farmacias e Drogarias",
        "4781400": "Artigos de Vestuario",
        "4782201": "Calcados",
        "4789004": "Gas Liquefeito de Petroleo",
        "4761003": "Livrarias",
    },
    "G45": {
        "4511101": "Comercio de Automoveis Novos",
        "4511102": "Comercio de Automoveis Usados",
        "4512901": "Representantes de Veiculos",
        "4541201": "Comercio de Motocicletas Novas",
        "4541206": "Comercio de Motocicletas Usadas",
        "4530701": "Comercio de Pecas e Acessorios",
        "4520001": "Servicos de Manutencao e Reparacao",
    },
    "G46": {
        "4611700": "Representantes Comerciais de Mat. Agricola",
        "4621400": "Comercio Atacadista de Cereais",
        "4631100": "Comercio Atacadista de Laticinios",
        "4641901": "Comercio Atacadista de Tecidos",
        "4649401": "Comercio Atacadista de Eletrodomesticos",
        "4661300": "Comercio Atacadista de Combustiveis",
        "4679699": "Outros Atacadistas",
    },
    "H49": {
        "4921301": "Transporte Rodoviario Urbano (municipio)",
        "4922101": "Transporte Rodoviario Intermunicipal",
        "4923001": "Taxi",
        "4924800": "Transporte Escolar",
        "4929901": "Outros Transportes Rodoviarios de Passageiros",
        "4930201": "Transporte Rodoviario de Cargas - Exceto Perigosas",
        "4930202": "Transporte Rodoviario de Cargas Perigosas",
        "4940000": "Dutos",
    },
    "H52": {
        "5211701": "Armazens Gerais - Emissao de Warrant",
        "5211799": "Depositos de Mercadorias para Terceiros",
        "5212500": "Carga e Descarga",
        "5229001": "Servicos de Apoio ao Transporte Rodoviario",
        "5229099": "Outros Servicos de Apoio ao Transporte",
    },
    "F41": {
        "4110700": "Incorporacao de Empreendimentos Imobiliarios",
        "4120400": "Construcao de Edificios",
    },
    "F42": {
        "4211101": "Construcao de Rodovias e Ferrovias",
        "4212000": "Construcao de Obras de Arte Especiais",
        "4221901": "Construcao de Barragens",
        "4222701": "Construcao de Redes de Abastecimento de Agua",
        "4223500": "Construcao de Redes de Transportes",
        "4229099": "Outras Obras de Infraestrutura",
    },
    "F43": {
        "4311801": "Demolicao e Preparacao de Terrenos",
        "4313400": "Terraplenagem",
        "4321500": "Instalacao e Manutencao Eletrica",
        "4322301": "Instalacoes Hidraulicas, Sanitarias e de Gas",
        "4329101": "Impermeabilizacao em Obras de Engenharia",
        "4330401": "Impermeabilizacao e Revestimento",
        "4330402": "Instalacao de Portas, Janelas e Esquadrias",
        "4391600": "Obras de Fundacoes",
        "4399101": "Administracao de Obras",
    },
    "N78": {
        "7810800": "Selecao e Agenciamento de Mao de Obra",
        "7820500": "Locacao de Mao de Obra (Terceirizacao)",
        "7830200": "Fornecimento e Gestao de Recursos Humanos",
    },
    "N80": {
        "8011101": "Atividades de Vigilancia e Seguranca Privada",
        "8011102": "Servicos de Adicionamento de Alarmes",
        "8012900": "Atividades de Transporte de Valores",
        "8020000": "Atividades de Monitoramento de Sistemas",
    },
    "N81": {
        "8111700": "Servicos Combinados para Apoio a Edificios",
        "8112500": "Condominio Predial",
        "8121400": "Limpeza em Predios e em Domicilios",
        "8122200": "Imunizacao e Controle de Pragas",
        "8129000": "Atividades de Limpeza Nao Especificadas",
        "8130300": "Atividades Paisagisticas",
    },
    "Q86": {
        "8610101": "Atividades de Atendimento Hospitalar (excl. UTI)",
        "8610102": "UTI Movel",
        "8621601": "UPA e Postos de Saude",
        "8630501": "Atividade Medica Ambulatorial (excl. Odontologia)",
        "8630502": "Laboratorio de Analises Clinicas",
        "8630503": "Servicos de Radiologia e Diagnostico por Imagem",
        "8640201": "Laboratorios de Analises Clinicas",
        "8650001": "Atividades de Enfermagem",
        "8650004": "Fisioterapia",
        "8660700": "Saude Animal",
    },
    "Q88": {
        "8800600": "Servicos de Assistencia Social sem Alojamento",
        "8711501": "Clinicas e Residencias Geriatricas",
        "8720499": "Centros de Assistencia Psicossocial",
    },
}


# ─── Auxiliares ───────────────────────────────────────────────────────────────

def save_json(name, data):
    path = DATA_DIR / f"{name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"Salvo: {path}")


def load_json(name, default=None):
    path = DATA_DIR / f"{name}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default if default is not None else {}


def mes_label(year, month):
    return f"{MESES_PT[month - 1]}/{year}"


# ─── Acumulacao de serie 12 meses ─────────────────────────────────────────────

def update_series(existing_json, year, month, adm, des, saldo, estoque):
    """Adiciona o mes atual na serie de 12 meses, mantendo janela deslizante."""
    label = mes_label(year, month)
    s = existing_json.get("series_12m", {
        "meses": [], "admissoes": [], "desligamentos": [], "saldo": [], "estoque": []
    })

    if label not in s["meses"]:
        s["meses"].append(label)
        s["admissoes"].append(adm)
        s["desligamentos"].append(des)
        s["saldo"].append(saldo)
        # Estoque: ultimo valor + saldo, ou saldo se nao houver anterior
        prev_est = s["estoque"][-1] if s["estoque"] else 50000
        s["estoque"].append(prev_est + saldo)

    # Mantém janela de 12 meses
    for key in ["meses", "admissoes", "desligamentos", "saldo", "estoque"]:
        if len(s[key]) > 12:
            s[key] = s[key][-12:]

    return s


# ─── Download CAGED ───────────────────────────────────────────────────────────

def latest_candidates():
    """Gera (ano, mes) do mais recente para o mais antigo (max 6 tentativas)."""
    now = datetime.now()
    for delta in range(1, 7):
        d = now - timedelta(days=30 * delta)
        yield d.year, d.month


def try_ftp(year, month, timeout=90):
    """
    Acessa ftp.mtps.gov.br via protocolo FTP real (porta 21).
    Esse servidor eh FTP nativo -- HTTP/HTTPS nao funcionam nele.
    """
    import ftplib
    ym = f"{year}{month:02d}"
    host = "ftp.mtps.gov.br"
    remote_dir = f"/pdet/microdados/NOVO CAGED/{year}/{ym}"
    filename = f"CAGEDMOV{ym}.7z"
    log.info(f"Tentando FTP: ftp://{host}{remote_dir}/{filename}")
    try:
        ftp = ftplib.FTP(host, timeout=timeout)
        ftp.login()
        ftp.cwd(remote_dir)
        buf = bytearray()
        ftp.retrbinary(f"RETR {filename}", buf.extend)
        ftp.quit()
        size_mb = len(buf) / 1e6
        log.info(f"FTP Download OK: {size_mb:.1f} MB")
        return bytes(buf)
    except Exception as exc:
        log.warning(f"Erro FTP {host}: {exc}")
    return None


def try_ftp_zip(year, month, timeout=90):
    """
    Tenta o arquivo .zip alternativo no mesmo servidor FTP.
    Alguns meses tem .zip em vez de .7z.
    """
    import ftplib
    ym = f"{year}{month:02d}"
    host = "ftp.mtps.gov.br"
    remote_dir = f"/pdet/microdados/NOVO CAGED/{year}/{ym}"
    filename = f"CAGEDMOV{ym}.zip"
    log.info(f"Tentando FTP (zip): ftp://{host}{remote_dir}/{filename}")
    try:
        ftp = ftplib.FTP(host, timeout=timeout)
        ftp.login()
        ftp.cwd(remote_dir)
        buf = bytearray()
        ftp.retrbinary(f"RETR {filename}", buf.extend)
        ftp.quit()
        size_mb = len(buf) / 1e6
        log.info(f"FTP (zip) Download OK: {size_mb:.1f} MB")
        return bytes(buf)
    except Exception as exc:
        log.warning(f"Erro FTP zip {host}: {exc}")
    return None


def build_http_urls(year, month):
    ym = f"{year}{month:02d}"
    y  = str(year)
    m2 = f"{month:02d}"
    return [
        # Portal BI MTE (HTTPS)
        f"https://bi.mte.gov.br/bgcaged/caged_ftp/public/md_caged_mov_{ym}.7z",
        f"https://bi.mte.gov.br/bgcaged/caged_ftp/public/CAGEDMOV{ym}.7z",
        # PDET MTE (HTTP, zip)
        f"http://pdet.mte.gov.br/assets/novo-caged/{y}-{m2}/1-CAGEDMOV{ym}.zip",
        # FTP via HTTPS (fallback improvavel mas vale tentar)
        f"https://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{y}/{ym}/CAGEDMOV{ym}.7z",
        f"http://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{y}/{ym}/CAGEDMOV{ym}.7z",
    ]


def try_http(url, timeout=60):
    """Baixa via HTTP/HTTPS com SSL desabilitado; retorna bytes ou None."""
    try:
        log.info(f"Tentando HTTP: {url}")
        r = requests.get(
            url, timeout=timeout, stream=True, verify=False,
            headers={"User-Agent": "ObservatorioJandira/1.1"}
        )
        if r.status_code == 200:
            buf = bytearray()
            for chunk in r.iter_content(65536):
                buf.extend(chunk)
            log.info(f"HTTP Download OK: {len(buf)/1e6:.1f} MB")
            return bytes(buf)
        log.warning(f"HTTP {r.status_code}: {url}")
    except Exception as exc:
        log.warning(f"Erro HTTP {url}: {exc}")
    return None


def download_caged(year, month):
    """
    Tenta baixar o arquivo CAGED para o mes indicado.
    Ordem: FTP nativo (.7z) -> FTP nativo (.zip) -> HTTP/HTTPS.
    Retorna bytes ou None.
    """
    # 1. FTP nativo -- mais confiavel, eh o protocolo correto desse servidor
    raw = try_ftp(year, month)
    if raw:
        return raw

    # 2. FTP nativo versao zip
    raw = try_ftp_zip(year, month)
    if raw:
        return raw

    # 3. HTTP/HTTPS como fallback
    for url in build_http_urls(year, month):
        raw = try_http(url, timeout=60)
        if raw:
            return raw

    return None


def extract_csv(data, year, month):
    """Extrai o primeiro CSV/TXT de um arquivo .7z ou .zip."""
    def _is_text(name):
        n = name.lower()
        return n.endswith(".csv") or n.endswith(".txt")

    # Tenta .7z
    if data[:6] == b"7z\xbc\xaf'\x1c":
        try:
            import py7zr
            with py7zr.SevenZipFile(io.BytesIO(data)) as zf:
                names = [n for n in zf.getnames() if _is_text(n)]
                log.info(f"Arquivos no .7z: {zf.getnames()}")
                if names:
                    extracted = zf.read(names[:1])
                    return list(extracted.values())[0].read().decode("latin-1")
        except Exception as exc:
            log.warning(f"Erro ao ler .7z: {exc}")

    # Tenta .zip
    if data[:2] == b"PK":
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                names = [n for n in zf.namelist() if _is_text(n)]
                log.info(f"Arquivos no .zip: {zf.namelist()}")
                if names:
                    return zf.read(names[0]).decode("latin-1")
        except Exception as exc:
            log.warning(f"Erro ao ler .zip: {exc}")

    return None


# ─── Processamento do CSV ─────────────────────────────────────────────────────

def parse_caged_csv(csv_text):
    """
    Le o CSV do CAGED, filtra Jandira e produz o dict para o dashboard.
    Retorna dict com campos em portugues, compativel com index.html.
    """
    try:
        import pandas as pd
    except ImportError:
        log.error("pandas nao instalado")
        return None

    log.info("Processando CSV do CAGED...")
    buf = io.StringIO(csv_text)
    chunks = []
    try:
        for chunk in pd.read_csv(
            buf, sep=";", chunksize=200_000,
            dtype={"Municipio": str, "Subclasse": str},
            low_memory=False
        ):
            chunk.columns = [c.strip() for c in chunk.columns]
            mun_col = next((c for c in chunk.columns if "munic" in c.lower()), None)
            if not mun_col:
                log.error(f"Coluna municipio nao encontrada. Colunas: {list(chunk.columns)}")
                return None
            sub = chunk[chunk[mun_col].astype(str).str.strip() == MUNICIPIO_STR]
            if len(sub) > 0:
                chunks.append(sub)
    except Exception as exc:
        log.error(f"Erro ao ler CSV: {exc}")
        return None

    if not chunks:
        log.warning("Nenhum registro para Jandira neste arquivo.")
        return None

    df = pd.concat(chunks, ignore_index=True)
    log.info(f"Registros de Jandira: {len(df)}")

    def find_col(keys):
        for k in keys:
            for c in df.columns:
                if k.lower() in c.lower():
                    return c
        return None

    col_tipo  = find_col(["tipo de mov", "tipodemov", "tipomov"])
    col_secao = find_col(["secao", "secção", "seção "])
    col_sub   = find_col(["subclasse"])
    col_sal   = find_col(["salariomensal", "salario mensal", "salário mensal"])
    col_instr = find_col(["grauinstrucao", "grau de inst", "instrucao"])
    col_sexo  = find_col(["sexo"])
    col_idade = find_col(["idade"])

    log.info(f"Colunas: tipo={col_tipo} secao={col_secao} sub={col_sub} "
             f"sal={col_sal} instr={col_instr} sexo={col_sexo} idade={col_idade}")

    ADM_TIPOS = {"10", "25", "50"}
    DES_TIPOS = {"20", "35", "40"}

    def cnt(series):
        t = series.astype(str).str.strip()
        return int(t.isin(ADM_TIPOS).sum()), int(t.isin(DES_TIPOS).sum())

    tipo_all = df[col_tipo].astype(str).str.strip() if col_tipo else None
    adm_total = int(tipo_all.isin(ADM_TIPOS).sum()) if tipo_all is not None else 0
    des_total = int(tipo_all.isin(DES_TIPOS).sum()) if tipo_all is not None else 0
    saldo_total = adm_total - des_total

    # ─── Por secao ────────────────────────────────────────────────────────────
    by_section = []
    if col_secao and col_tipo:
        for sec, grp in df.groupby(col_secao):
            sec = str(sec).strip()
            adm, des = cnt(grp[col_tipo])
            saldo = adm - des

            # Salario medio (admissoes apenas)
            sal_medio = 0
            if col_sal:
                try:
                    adm_mask = grp[col_tipo].astype(str).str.strip().isin(ADM_TIPOS)
                    sal_medio = round(
                        float(pd.to_numeric(grp.loc[adm_mask, col_sal], errors="coerce").mean()), 0
                    )
                except Exception:
                    sal_medio = 0

            # Escolaridade
            por_instrucao = []
            pct_superior = 0
            if col_instr:
                try:
                    inst_map = {
                        1: "Analfabeto", 2: "Fund. Incompleto",
                        3: "Fund. Completo", 4: "Medio Incompleto",
                        5: "Medio Completo", 6: "Sup. Incompleto",
                        7: "Sup. Completo", 8: "Mestrado", 9: "Doutorado"
                    }
                    total_grp = len(grp)
                    counts = grp[col_instr].value_counts()
                    for code, label_inst in inst_map.items():
                        n = int(counts.get(code, 0))
                        pct = round(n * 100 / total_grp, 1) if total_grp > 0 else 0
                        por_instrucao.append({"label": label_inst, "pct": pct})
                    # pct superior completo
                    n_sup = int(counts.get(7, 0))
                    pct_superior = round(n_sup * 100 / total_grp, 1) if total_grp > 0 else 0
                except Exception:
                    pass

            # Genero
            por_genero = {"M": 0, "F": 0}
            if col_sexo:
                try:
                    sexo = grp[col_sexo].astype(str).str.strip()
                    total_grp = len(grp)
                    nm = int((sexo == "1").sum())
                    nf = int((sexo == "3").sum())
                    por_genero = {
                        "M": round(nm * 100 / total_grp) if total_grp else 0,
                        "F": round(nf * 100 / total_grp) if total_grp else 0
                    }
                except Exception:
                    pass

            # Faixa etaria
            faixa_etaria = []
            if col_idade:
                try:
                    idades = pd.to_numeric(grp[col_idade], errors="coerce")
                    for lo, hi, lbl in [
                        (18, 24, "18-24"), (25, 34, "25-34"),
                        (35, 44, "35-44"), (45, 54, "45-54"), (55, 99, "55+")
                    ]:
                        faixa_etaria.append({"label": lbl, "n": int(((idades >= lo) & (idades <= hi)).sum())})
                except Exception:
                    pass

            details = {
                "por_instrucao": por_instrucao,
                "por_genero": por_genero,
                "faixa_etaria": faixa_etaria,
            }

            # ─── Divisoes ─────────────────────────────────────────────────────
            divisions = []
            div_labels = DIV.get(sec, {})
            if col_sub and div_labels:
                div_map = {}
                for subcode, subgrp in grp.groupby(col_sub):
                    subcode = str(subcode).strip()
                    div_key = subcode[:2] if len(subcode) >= 2 else subcode
                    if div_key not in div_labels:
                        continue
                    a2, d2 = cnt(subgrp[col_tipo])
                    if div_key not in div_map:
                        div_map[div_key] = {
                            "div": f"{sec}{div_key}",
                            "label": div_labels[div_key],
                            "admissoes": 0, "desligamentos": 0, "saldo": 0,
                            "_subs": {}
                        }
                    div_map[div_key]["admissoes"] += a2
                    div_map[div_key]["desligamentos"] += d2
                    div_map[div_key]["saldo"] += a2 - d2

                    # Subclasses desta divisao
                    sub_dict_key = f"{sec}{div_key}"
                    if sub_dict_key in SUB:
                        lbl_sub = SUB[sub_dict_key].get(subcode)
                        if lbl_sub:
                            if subcode not in div_map[div_key]["_subs"]:
                                div_map[div_key]["_subs"][subcode] = {
                                    "sub": subcode, "label": lbl_sub,
                                    "admissoes": 0, "desligamentos": 0, "saldo": 0
                                }
                            div_map[div_key]["_subs"][subcode]["admissoes"] += a2
                            div_map[div_key]["_subs"][subcode]["desligamentos"] += d2
                            div_map[div_key]["_subs"][subcode]["saldo"] += a2 - d2

                # Monta lista de divisoes ordenada por |saldo|
                for dk, dv in sorted(div_map.items(), key=lambda x: abs(x[1]["saldo"]), reverse=True):
                    entry = {
                        "div": dv["div"],
                        "label": dv["label"],
                        "admissoes": dv["admissoes"],
                        "desligamentos": dv["desligamentos"],
                        "saldo": dv["saldo"],
                    }
                    subs = list(dv["_subs"].values())
                    if subs:
                        entry["subclasses"] = sorted(subs, key=lambda x: abs(x["saldo"]), reverse=True)
                    divisions.append(entry)

            entry_sec = {
                "secao": sec,
                "label": SECAO_LABEL.get(sec, sec),
                "admissoes": adm,
                "desligamentos": des,
                "saldo": saldo,
                "salario_medio": sal_medio,
                "pct_superior": pct_superior,
                "details": details,
                "divisions": divisions,
            }
            by_section.append(entry_sec)

        by_section.sort(key=lambda x: abs(x["saldo"]), reverse=True)

    return {
        "admissoes": adm_total,
        "desligamentos": des_total,
        "saldo": saldo_total,
        "total_records": len(df),
        "by_section": by_section,
    }


# ─── IBGE SIDRA ───────────────────────────────────────────────────────────────

def fetch_sidra(table_id, var_id, period="last 1"):
    url = (
        f"https://servicodados.ibge.gov.br/api/v3/agregados/{table_id}"
        f"/periodos/{period}/variaveis/{var_id}"
        f"?localidades=N6[{MUNICIPIO_STR}]&formato=JSON"
    )
    try:
        r = requests.get(url, timeout=30, verify=True)
        r.raise_for_status()
        data = r.json()
        series = data[0]["resultados"][0]["series"]
        if series:
            return {k: v for k, v in series[0]["serie"].items()}
    except Exception as exc:
        log.warning(f"SIDRA t={table_id} v={var_id}: {exc}")
    return {}


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Observatorio Economico de Jandira - Pipeline de dados ===")
    now = datetime.utcnow().isoformat() + "Z"

    existing = load_json("caged_latest", {})

    meta = {
        "last_updated": now[:10],
        "sources": {},
        "pipeline_version": "2.0",
        "notes": "Dados atualizados automaticamente via GitHub Actions todo dia 25."
    }

    # ── CAGED ─────────────────────────────────────────────────────────────────
    log.info("Buscando Novo CAGED (MTE)...")
    caged_result = None
    caged_year = caged_month = None

    force_month = os.environ.get("FORCE_MONTH", "").strip()
    if force_month:
        try:
            y, m = [int(x) for x in force_month.split("-")]
            candidates = [(y, m)]
            log.info(f"Forca mes: {y}-{m:02d}")
        except Exception:
            candidates = list(latest_candidates())
    else:
        candidates = list(latest_candidates())

    for year, month in candidates:
        raw = download_caged(year, month)
        if not raw:
            log.warning(f"Todos os metodos falharam para {year}-{month:02d}. Tentando mes anterior...")
            continue
        csv_text = extract_csv(raw, year, month)
        if not csv_text:
            log.warning("Arquivo baixado mas nao foi possivel extrair CSV.")
            continue
        result = parse_caged_csv(csv_text)
        if result:
            caged_result = result
            caged_year = year
            caged_month = month
            break

    if caged_result:
        label = mes_label(caged_year, caged_month)
        ref_month = f"{caged_year}-{caged_month:02d}"

        series = update_series(
            existing, caged_year, caged_month,
            caged_result["admissoes"],
            caged_result["desligamentos"],
            caged_result["saldo"],
            estoque=None
        )

        caged_out = {
            "reference_month": ref_month,
            "reference_month_label": label,
            "municipio": MUNICIPIO_STR,
            "municipio_nome": "Jandira",
            "series_12m": series,
            "by_section": caged_result["by_section"],
        }
        save_json("caged_latest", caged_out)
        log.info(f"CAGED {ref_month}: Adm={caged_result['admissoes']} "
                 f"Des={caged_result['desligamentos']} Saldo={caged_result['saldo']}")

        meta["sources"]["caged"] = {
            "label": "Novo CAGED / MTE",
            "reference_month": ref_month,
            "reference_month_label": label,
            "downloaded_at": now,
            "records_jandira": caged_result["total_records"],
            "status": "ok"
        }
    else:
        log.warning("Nao foi possivel baixar CAGED. Mantendo dados anteriores.")
        if existing:
            save_json("caged_latest", existing)
        meta["sources"]["caged"] = {
            "label": "Novo CAGED / MTE",
            "downloaded_at": now,
            "status": "falha - mantendo dados anteriores"
        }

    # ── IBGE SIDRA ────────────────────────────────────────────────────────────
    log.info("Buscando IBGE SIDRA...")
    sidra_pop = fetch_sidra(9514, 93)
    meta["sources"]["sidra_pop"] = {
        "label": "IBGE SIDRA - Projecao Populacional",
        "reference_year": 2024,
        "downloaded_at": now,
        "status": "ok" if sidra_pop else "sem dados"
    }

    save_json("metadata", meta)
    log.info("=== Pipeline concluido ===")


if __name__ == "__main__":
    main()

import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import unicodedata
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import time
import random

# ----------------------- EXTRACT -----------------------
def extract():
    titulo = 'data'
    loc = 'br'
    start = 0
    finish = 1000
    f_TPR = "r604800"  # Filtro: até uma semana atrás

    lista_de_vagas = []

    # Headers para simular navegador
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.90 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.linkedin.com/"
    }

    # Função com retentativas para 429
    def fetch_with_retry(url, headers, max_retries=5):
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 429:
                    wait = 2 ** attempt  # Tempo de espera exponencial
                    print(f"Erro 429: Esperando {wait} segundos...")
                    time.sleep(wait)
                else:
                    response.raise_for_status()
                    return response
            except requests.exceptions.RequestException as e:
                print(f"Erro ao buscar: {e}")
                time.sleep(2)
        return None

    # Coleta de vagas
    while start < finish:
        list_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={titulo}&location={loc}&f_TPR={f_TPR}&start={start}"
        response = fetch_with_retry(list_url, headers)

        if response:
            list_soup = BeautifulSoup(response.text, "html.parser")
            lista_de_vagas.extend(list_soup.find_all('li'))
            start += 10
            time.sleep(random.uniform(2, 5))  # Espera aleatória entre requisições
        else:
            break

    print(f"Total de vagas coletadas: {len(lista_de_vagas)}")

    lista_de_ids = []
    for vaga in lista_de_vagas:
        base_card_full_link = vaga.find("div", {"class": "base-card"})
        if base_card_full_link and base_card_full_link.get("data-entity-urn"):
            id_vaga = base_card_full_link.get("data-entity-urn").split(":")[3]
            lista_de_ids.append(id_vaga)

    lista_vagas = []

    for id in lista_de_ids:
        url_vaga = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{id}"
        vaga_response = fetch_with_retry(url_vaga, headers)
        if not vaga_response:
            continue

        vaga_soup = BeautifulSoup(vaga_response.text, "html.parser")
        vaga_info = {}

        # Empresa
        vaga_info['empresa'] = vaga_soup.find("img", {"class": "artdeco-entity-image"}).get("alt", None)

        # Título da vaga
        titulo_vaga = vaga_soup.find("h2", {"class": "top-card-layout__title"})
        vaga_info['vaga'] = titulo_vaga.text.strip() if titulo_vaga else None

        # Data postada
        data_postagem = vaga_soup.find("span", {"class": "posted-time-ago__text"})
        vaga_info["data"] = data_postagem.text.strip() if data_postagem else None

        # Link
        vaga_info['link'] = url_vaga

        # Descrição + critérios
        descricao = vaga_soup.find("div", {"class": "show-more-less-html__markup"})
        criterios = vaga_soup.find("div", {"class": "description__job-criteria-list"})
        combined_desc = ''
        if descricao:
            combined_desc += descricao.get_text(separator=" ", strip=True)
        if criterios:
            combined_desc += " " + criterios.get_text(separator=" ", strip=True)

        vaga_info['descricao'] = combined_desc if combined_desc else None
        lista_vagas.append(vaga_info)

    # Cria DataFrame
    df = pd.DataFrame(lista_vagas)

    # Filtra vagas por palavras-chave
    palavras_chave = ['dado', 'data', 'analis', 'analys']
    regex = '|'.join(palavras_chave)
    df = df[df['vaga'].str.contains(regex, case=False, na=False)].reset_index(drop=True)

    return df

# ----------------------- TRANSFORM -----------------------
def transform(df):
    def limpar_texto(texto):
        if isinstance(texto, str):
            texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
            return re.sub(r'[^a-zA-Z0-9\s]', '', texto).lower()
        return texto

    df = df.apply(lambda col: col if col.name == 'link' else col.apply(limpar_texto))

    # Converter datas
    hoje = datetime.today()
    def converter_data(texto):
        if pd.isnull(texto):
            return hoje.strftime('%d/%m/%Y')
        match = re.search(r'(\d+)\sday[s]?', texto)
        if match:
            dias = int(match.group(1))
            nova_data = hoje - timedelta(days=dias)
            return nova_data.strftime('%d/%m/%Y')
        return hoje.strftime('%d/%m/%Y')

    df['data'] = df['data'].apply(converter_data)
    df['data'] = pd.to_datetime(df['data'], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")

    # Detectar senioridade
    senioridades = ['estagio', 'estagiario', 'assistente', 'junior', 'pleno', 'senior', 'jr', 'sr', 'pl', 'pleno-senior']
    def identificar_senioridade(texto):
        texto_lower = texto.lower()
        for nivel in senioridades:
            if re.search(rf'\b{nivel}\b', texto_lower):
                return nivel.capitalize()
        return 'Não especificado'

    df['senioridade'] = df.apply(lambda x: identificar_senioridade(f"{x['descricao']} {x['vaga']}"), axis=1)

    # Filtrar skills
    skills_tools = {
        # Linguagens de Programação
        'python', 'r', 'sql', 'scala', 'java', 'c++', 'c#', 'javascript', 'typescript', 'go', 'bash',

        # Frameworks & Bibliotecas
        'pandas', 'numpy', 'matplotlib', 'seaborn', 'scikit-learn', 'statsmodels', 'tensorflow', 'keras', 
        'pytorch', 'xgboost', 'lightgbm', 'catboost', 'nltk', 'spacy', 'openai', 'transformers',

        # BI & Visualização
        'power bi', 'tableau', 'looker', 'qlikview', 'metabase', 'superset',

        # Cloud & Big Data
        'aws', 'azure', 'google cloud', 'databricks', 'redshift', 'bigquery', 'snowflake', 'hadoop', 'spark', 
        'hive', 'data lake', 's3', 'lambda', 'emr',

        # ETL, Orquestração & DataOps
        'airflow', 'luigi', 'talend', 'informatica', 'dbt', 'nifi', 'kafka', 'flink', 'kinesis', 'glue',

        # Bancos de Dados
        'mysql', 'postgresql', 'sql server', 'oracle', 'mongodb', 'redis', 'cassandra', 'neo4j', 'dynamodb',

        # DevOps & Infraestrutura
        'docker', 'kubernetes', 'terraform', 'ansible', 'jenkins', 'ci/cd', 'git', 'github', 'gitlab', 'bitbucket',

        # Inteligência Artificial & Ciência de Dados
        'machine learning', 'deep learning', 'nlp', 'computer vision', 'estatística', 'probabilidade', 
        'regressão', 'classificação', 'clusterização', 'modelagem preditiva', 'análise exploratória', 
        'engenharia de atributos', 'visualização de dados', 'otimização', 'séries temporais', 'reinforcement learning',

        # Data Governance & Segurança
        'data governance', 'lgpd', 'gdpr', 'segurança da informação', 'mascaramento de dados', 'data lineage',

        # Metodologias & Conceitos
        'scrum', 'kanban', 'agile', 'lean', 'six sigma', 'okrs', 'kpi', 'etl', 'elt', 'data warehouse', 
        'data lakehouse', 'data mesh', 'arquitetura de dados', 'agil', 'ageis',

        # Soft Skills
        'trabalho em equipe', 'comunicação', 'resolução de problemas', 'pensamento crítico', 'criatividade',
        'liderança', 'gestão de tempo', 'empatia', 'proatividade', 'adaptabilidade', 'tomada de decisão', 
        'negociação', 'gestão de projetos', 'aprendizado contínuo', 'análise estratégica', 'foco em resultados', 
        'colaboração', 'inteligência emocional', 'pensamento analítico', 'orientação a detalhes'
    }

    def filtrar_skills(texto):
        texto_lower = texto.lower()
        return [skill for skill in skills_tools if skill in texto_lower]

    df['descricao'] = df['descricao'].apply(filtrar_skills)

    return df

# ----------------------- LOAD -----------------------
def load_data(df):
    """ Carrega dados no PostgreSQL """
    DB_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/li_etl"
    
    try:
        engine = create_engine(DB_URL)
        df.to_sql('vagas', engine, if_exists='append', index=False)
        print("✅ Dados carregados com sucesso!")
    except Exception as e:
        print(f"Erro ao carregar dados no banco: {e}")

# ----------------------- EXECUÇÃO -----------------------
if __name__ == "__main__":
    df = extract()
    if not df.empty:
        df_transformed = transform(df)
        load_data(df_transformed)
    else:
        print("⚠️ Nenhuma vaga encontrada ou erro durante a extração.")
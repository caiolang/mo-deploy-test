import marimo

__generated_with = "0.15.2"
app = marimo.App(width="columns")


@app.cell(column=0)
def _(get_df_long, mo, pl):
    # Leitura do df original em CSV
    base_1 = str(
        mo.notebook_location()
        / "public"
        / "Carol_DataBaseFull_21052025_anonimizado.csv"
    )
    base_2 = str(
        mo.notebook_location() / "public" / "Carol_DataBaseFull_Limpa.csv"
    )
    df_original = pl.read_csv(base_1)
    # Join com a versão anterior dos dados por conta de colunas que desapareceram na nova versão
    df_old = pl.read_csv(base_2)
    df_original = df_original.join(df_old, on=["id_family_datalake"])

    # Passa o dataframe para o formato long (uma row por resposta, ao invés de uma row por família)
    df_long = get_df_long(df_original)
    df_long.write_csv(str(mo.notebook_location() / "public" / "df_long.csv"))
    return


@app.cell
def _():
    ASSERTION_MAP = {
        "Access": {
            "map": {
                "Existe algum membro que não tem acesso": [
                    "Sim, existe algum membro que não tem acesso"
                ],
                "Todos os membros tem acesso": [
                    "Não, todos os membros tem acesso",
                ],
            },
            "order": [
                "NA",
                "Existe algum membro que não tem acesso",
                "Todos os membros tem acesso",
            ],
        },
        "SchoolCurrent": {
            "map": {
                "Não": [
                    "Nao",
                    "Não sabe",
                ],
            },
            "order": [
                "NA",
                "Não",
                "Sim",
            ],
        },
        "FoodManytimes": {
            "map": {
                "Mais que 3 vezes ao dia": [
                    "mais que 3 vezes ao dia",
                ],
                "1 ou 2 vezes ao dia": [
                    "1 vez ao dia",
                    "2 vezes ao dia",
                ],
            },
            "order": [
                "NA",
                "1 ou 2 vezes ao dia",
                "3 vezes ao dia",
                "Mais que 3 vezes ao dia",
            ],
        },
        "Eletricity": {
            "map": {
                "Não": [
                    "Nao",
                    "Não sabe",
                    "Outro",
                ],
                "Possui sem padrão próprio": [
                    "Possui sem padrao proprio",
                ],
                "Possui com padrão próprio": [
                    "Possui com padrao proprio",
                ],
            },
            "order": [
                "NA",
                "Não",
                "Possui sem padrão próprio",
                "Possui com padrão próprio",
            ],
        },
        "Floor": {
            "map": {
                "Cerâmica, lajota, pedra, material sustentável e/ou madeira trabalhada": [
                    "Ceramica Lajota ou Pedra",
                    "Madeira Trabalhada",
                    "Material sustentável",
                ],
                "Terra batida": [
                    "Terra Batida",
                ],
                "Madeira aproveitada": [
                    "Madeira Aproveitada",
                ],
                "Cimento e/ou contrapiso": [
                    "Cimento / Contrapiso",
                    "Outro Material",
                ],
            },
            "order": [
                "NA",
                "Terra batida",
                "Madeira aproveitada",
                "Cimento e/ou contrapiso",
                "Cerâmica, lajota, pedra, material sustentável e/ou madeira trabalhada",
            ],
        },
        "CEP": {
            "map": {
                "Tenho CEP": [
                    "Sim, tenho CEP (Código de Endereçamento Postal)",
                ],
                "Não tenho CEP": [
                    "Não, não tenho CEP (Código de Endereçamento Postal)",
                ],
            },
            "order": [
                "NA",
                "Não tenho CEP",
                "Tenho CEP",
            ],
        },
        "StayFavela": {
            "order": [
                "NA",
                "Não",
                "Não sei",
                "Sim",
            ]
        },
        "DreamsKids": {
            "map": {
                "Não": [
                    "(espontâneo) Não sei",
                    "Nao Sei",
                    "Nao",
                ],
                "Sim": [
                    "Sim, o que?",
                    "Sim o que?",
                ],
            },
            "order": [
                "NA",
                "Não",
                "Sim",
            ],
        },
        "FamilyRelations": {
            "order": [
                "NA",
                "Conflituosas, com violência",
                "Eventualmente há violência",
                "Conflituosas, sem violência",
                "Sem conflitos relevantes",
                "Harmônicas",
            ],
        },
        "EventsFrequency": {
            "order": [
                "NA",
                "Nunca",
                "Às vezes",
                "Com frequência",
                "Sempre",
            ]
        },
        "Satisfaction": {
            "map": {},
            "order": [
                "NA",
                "Muito insatisfeito",
                "Insatisfeito",
                "Mais ou menos",
                "Satisfeito",
                "Muito satisfeito",
            ],
        },
        "Water": {
            "map": {
                "Encanada de poço ou nascente": [
                    "Encanada de Poco / Nascente",
                    "Cisterna",
                ],
                "Busco com balde": [
                    "Busco com Balde",
                ],
                "Carro-pipa": [
                    "Carro Pipa",
                ],
                "Encanada fora da rede oficial": [
                    "Encanada Clandestina",
                ],
                "Encanada da rede pública": [
                    "Rede Publica",
                ],
            },
            "order": [
                "NA",
                "Outro",
                "Busco com balde",
                "Carro-pipa",
                "Encanada fora da rede oficial",
                "Encanada de poço ou nascente",
                "Encanada da rede pública",
            ],
        },
        "Walls": {
            "map": {
                "Outro": [
                    "Não sabe",
                ],
                "Madeira aproveitada": [
                    "Madeira Aproveitada",
                ],
                "Taipa ou alvenaria sem revestimento": [
                    "Alvenaria / Tijolo SemRevestimento",
                    "Taipa",
                    "Taipa/Alvenaria e Tijolo Sem Revestimento",
                ],
                "Materiais adequados": [
                    "Paineis estruturados",
                    "Alvenaria / Tijolo Com Revestimento",
                ],
            },
            "order": [
                "NA",
                "Outro",
                "Lona",
                "Palha",
                "Madeira aproveitada",
                "Taipa ou alvenaria sem revestimento",
                "Materiais adequados",
            ],
        },
        "BankAccount": {
            "map": {
                "Sim": [
                    "Sim",
                    "Sim em meu nome",
                ],
                "Não": [
                    "Nao",
                    "Nao sei",
                    "Nao Sei",
                ],
            },
            "order": [
                "NA",
                "Não",
                "Sim mas não em meu nome",
                "Sim",
            ],
        },
        "CulturalEvent": {
            "map": {},
            "order": [
                "NA",
                "Nunca",
                "Às vezes",
                "Com frequência",
                "Sempre",
            ],
        },
        "SchoolLiteracy": {
            "map": {
                "Não sei ler": [
                    "Não sei informar",
                ],
            },
            "order": [
                "NA",
                "Não sei ler",
                "Não muito bem",
                "Bem",
                "Muito bem",
            ],
        },
        "IncomeWorkS3": {
            "map": {
                "Trabalho informal": [
                    "Trabalho Informal",
                ],
                "Autônomo": [
                    "Autonomo",
                ],
                "Não estou trabalhando": [
                    "Eu não estou trabalhando",
                    "Não sei",
                ],
                "CLT, servidor público, estágio ou jovem aprendiz": [
                    "CLT",
                    "Funcionário Público Concursado",
                    "Servidor público",
                    "Estágio/jovem aprendiz",
                ],
            },
            "order": [
                "NA",
                "Não estou trabalhando",
                "Trabalho informal",
                "Autônomo",
                "CLT, servidor público, estágio ou jovem aprendiz",
                "Aposentado",
            ],
        },
        "WhyNotWork": {
            "map": {
                "Estou aposentado": [
                    "Estou aposentado/a",
                ],
                "Estou cuidando de alguém da família ou não tenho com quem deixar meus filhos": [
                    "Está cuidando de alguém da família e por isso não consigue trabalhar",
                    "Não tenho com quem deixar meus filhos",
                ],
                "Estou com um problema de saúde": [
                    "Estou com um problema de saúde que me impossibilita/dificulta trabalhar"
                ],
                "Sou dona de casa": [
                    "Sou dona/o de casa",
                ],
                "Estou buscando trabalho mas não encontro": [
                    "Estou buscando emprego e trabalho ativamente mas não encontro"
                ],
                "Outro": [
                    "Não sei",
                    "Outro. O que?",
                ],
            },
            "order": [
                "NA",
                "Outro",
                "Estou em situação de drogadição",
                "Estou com um problema de saúde",
                "Estou cuidando de alguém da família ou não tenho com quem deixar meus filhos",
                "Sou dona de casa",
                "Estou buscando trabalho mas não encontro",
                "Estou estudando",
                "Estou aposentado",
            ],
        },
        "SchoolLast": {
            "map": {
                "Nunca estudei": [
                    "Não sei",
                ],
                "EJA": [
                    "Alfabetização para adultos",
                    "Alfabetização para adutlos",
                    "Educação de Jovens e Adultos",
                    "EJA",
                ],
                "Técnico profissionalizante": [
                    "Técnico/profissionalizante",
                    "Técnico / Profissionalizante",
                ],
            },
            "order": [
                "NA",
                "Nunca estudei",
                "Pré-escola",
                "Ensino fundamental",
                "EJA",
                "Ensino médio",
                "Técnico profissionalizante",
                "Superior",
            ],
        },
        "Sewer": {
            "map": {
                "Outro": [
                    "Não sabe",
                ],
                "Céu aberto": [
                    "Ceu Aberto",
                ],
                "Ligado à rede não oficial": [
                    "Ligado Rede Não Oficial",
                    "Ligado Rede Nao Oficial",
                ],
                "Ligado à rede oficial": [
                    "Ligado Rede Oficial",
                ],
            },
            "order": [
                "NA",
                "Outro",
                "Céu aberto",
                "Ligado à rede não oficial",
                "Fossa",
                "Fossa séptica",
                "Ligado à rede oficial",
            ],
        },
        "SchoolMathLit": {
            "map": {
                "Não sei fazer contas matemáticas": [
                    "Não sei informar",
                ],
            },
            "order": [
                "NA",
                "Não sei fazer contas matemáticas",
                "Não muito bem",
                "Bem",
                "Muito bem",
            ],
        },
        "WaterFrequency": {
            "map": {
                "Não tenho": [
                    "Nao Tenho",
                ],
            },
            "order": [
                "NA",
                "Não tenho",
                "Menos de dois dias por semana",
                "Entre dois e quatro dias por semana",
                "Entre quatro e seis dias por semana",
                "Todos os dias da semana",
            ],
        },
        "Bathroom": {
            "map": {
                "Não tem": [
                    "Nao Tem",
                ],
                "Um banheiro compartilhado": [
                    "Um Banheiro Compartilhado",
                ],
                "Um banheiro exclusivo da família": [
                    "Um Banheiro Exclusivo",
                ],
                "Mais de um banheiro exclusivo da família": [
                    "Mais de um Banheiro Exclusivo"
                ],
            },
            "order": [
                "NA",
                "Outro",
                "Não tem",
                "Um banheiro compartilhado",
                "Um banheiro exclusivo da família",
                "Mais de um banheiro exclusivo da família",
            ],
        },
        "Roof": {
            "map": {
                "Materiais adequados": [
                    "Telhas de Fibrocimento Com Manta",
                    "Telha Metalica",
                    "Laje",
                    "Lajes com Impermeabilizacao",
                    "Painéis estruturados",
                    "Telha de Barro  / Ceramica",
                ],
                "Telhas sem manta": [
                    "Telhas de Fibrocimento Sem Manta",
                ],
                "Outro material": [
                    "Outro Material",
                    "Não sabe",
                ],
                "Madeira aproveitada": [
                    "Madeira Aproveitada",
                ],
            },
            "order": [
                "NA",
                "Outro material",
                "Lona",
                "Palha",
                "Madeira aproveitada",
                "Telhas sem manta",
                "Materiais adequados",
            ],
        },
        "Documents": [
            {
                "map": {
                    "Registro Nacional de Estrangeiro (RNE)": [
                        "Permisso de entrada, Autorização de residência, Protocolo de situação de Refugio, RNE, RME, Refugiado",
                        "RNE",
                    ],
                    "Registro indígena": [
                        "Registro Administrativo de Nascimento Indígena",
                        "Registro indígena",
                    ],
                    "RG": [
                        "Carteira de Identidade",
                        "RG",
                    ],
                    "Cartão SUS": ["Cartão SUS", "SUS"],
                    "Certidão de nascimento": ["Certidão de Nascimento"],
                    "Não tenho nenhum documento": [
                        "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                    ],
                },
                "filter_by_order": True,  # [TODO] Ainda não é usado
                "order": [
                    # "0", # 13 respostas (só tempo incial)
                    # "1", # 45 respostas (só tempo incial)
                    "Não tenho nenhum documento",
                    "CPF",
                    "Carteira de trabalho",
                    "Certidão de nascimento",
                    "Certidão de casamento",
                    "Cartão SUS",
                    "RG",
                    "Certificado de reservista",
                    "NIS/NIT",
                    "Título de eleitor",
                    "Registro indígena",
                    "Registro Nacional de Estrangeiro (RNE)",
                ],
            }
        ],
        "HealthGenKidsNames": {
            "map": {"Diarréia crônica": ["Diarréia Crônica"]},
            "order": [
                "NA",
                "Problemas de saúde bucal",
                "Doenças causadas por insetos",
                "Doenças respiratórias",
                "Diarréia crônica",
                "Desnutrição",
            ],
        },
        "HealthGenNames": {
            "map": {"Diarréia crônica": ["Diarréia Crônica"]},
            "order": [
                "NA",
                "Problemas de saúde bucal",
                "Doenças causadas por insetos",
                "Doenças respiratórias",
                "Diarréia crônica",
                "Desnutrição",
            ],
        },
        "Internet": [
            {
                "subtitle": "Todas as respostas",
                "map": {
                    "Não tenho acesso à internet": [
                        "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                    ]
                },
                "order": [
                    "Não tenho acesso à internet",
                    "Para acessar banco e serviços financeiros",
                    "Para acessar serviços públicos (poupa tempo, gov.br, INSS, etc...)",
                    "Para comprar coisas",
                    "Para estudar",
                    "Para falar com amigos e família",
                    "Para passar o tempo (redes sociais, jogos, ouvir música, ver filme...)",
                    "Para trabalhar",
                ],
            },
            # {
            #     "subtitle": "Binário: acesso à internet",  # [TODO] aqui precisaria re-obter uma linha por família, senão cada família vai ter várias asserções somando em "Tenho acesso à internet" por exemplo :ooo
            #     "map": {
            #         "Tenho acesso à internet": [
            #             "Para acessar banco e serviços financeiros",
            #             "Para acessar serviços públicos (poupa tempo, gov.br, INSS, etc...)",
            #             "Para comprar coisas",
            #             "Para estudar",
            #             "Para falar com amigos e família",
            #             "Para passar o tempo (redes sociais, jogos, ouvir música, ver filme...)",
            #             "Para trabalhar",
            #         ]
            #     },
            #     "order": ["Não tenho acesso à internet", "Tenho acesso à internet"],
            # },
        ],
        "CommFacilities": {
            "map": {
                "Iluminação pública": [
                    "Iluminacao Publica",
                    "Iluminação Pública",
                ],
                "Hospital público": [
                    "Hospital Publico",
                    "Hospital Público",
                ],
                "Creche ou escola pública": [
                    "Coleta de Lixo",
                    "Creche Publica",
                    "Creche Pública",
                    "Escola Publica",
                    "Escola Pública",
                ],
                "Opções de lazer": [
                    "Opcoes de Lazer",
                    "Opções de lazer",
                ],
                "Transporte Público": [
                    "Transporte Publico",
                    "Transporte Público",
                ],
                "Esgoto e água encanada": [
                    "Acesso à rede de esgoto",
                    "Agua Encanada",
                    "Esgoto",
                    "Água encanada",
                ],
                "Espaços comunitários": [
                    "Espaços para reuniões comunitárias",
                ],
                "Ruas e vielas": [
                    "Boas condições das ruas, vielas ou escadas que dão acesso à comunidade",
                    "Pavimentação das ruas e vielas da comunidade",
                ],
                "Posto de Saúde": [
                    "Posto de Saude",
                    "Posto de Saúde",
                ],
            },
            "order": [
                "Nenhuma opção",
                "Não sabe",
                "Iluminação pública",
                "Hospital público",
                "Creche ou escola pública",
                "Opções de lazer",
                "Transporte Público",
                "Esgoto e água encanada",
                "Espaços comunitários",
                "Ruas e vielas",
                "Posto de Saúde",
            ],
        },
        "BathroomQualit": {
            "map": {},
            "order": [
                "Box ou cortina que fecha o chuveiro",
                "Chuveiro com água quente",
                "Parede de azulejo",
                "Piso de azulejo",
                "Porta externa que fecha o banheiro",
                "Privada com tampa",
            ],
        },
        "HousingProblems": {
            "map": {
                "Infiltração, alagamento, inundação, umidade, chuva, goteiras e mofo": [
                    "Infiltração",
                    "Infiltração e humildade",
                    "Chuva Goteiras",
                    "Goteira",
                    "Umidade Mofo",
                    "Alagamento Inundacao",
                ],
                "Deslizamento, desmoronamento, solapamento ou casa caindo": [
                    "Deslizamento",
                    "Desmoronamento",
                    "Solapamento",
                    "Casa caindo",
                ],
                "Nenhum problema": [
                    "Minha casa não tem nenhum problema",
                    "Nenhum risco",
                    "Não",
                    "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                ],
                "Animais indesejados": [
                    "Escorpião, Embuá",
                    "Ratos Baratas Animais Indesejados",
                    "Gambá",
                    "Mosquito",
                ],
                # "Rachaduras e vazamentos": [
                # ],
                # "Saneamento básico": [
                # ],
                "Risco de incêndio": [
                    "Incendio",
                ],
                "Outros": [
                    "1",  # ???
                    "A escada de entrada.",
                    "Cozinha Com Lenha",
                    "Tiroteio",
                    "Espaço pequeno",
                    "Poste de iluminação pública com risco de cair",
                    "Espaço pequeno",
                    "Outro",
                    #
                    "Esgoto entupido",
                    "Falta de água constante",
                    "Saneamento básico",
                    #
                    "Banheiro com vazamento",
                    "Vazamentos hidráulicos",
                    "Rachadura",
                    "rachaduras",
                ],
            },
            "order": [
                "Outros",
                "Deslizamento, desmoronamento, solapamento ou casa caindo",
                "Infiltração, alagamento, inundação, umidade, chuva, goteiras e mofo",
                "Animais indesejados",
                "Risco de incêndio",
                "Cupim",
                "Nenhum problema",
            ],
        },
        "Garbage": {
            "map": {
                "Joga na rua, vala ou quintal": [
                    "Joga na rua / vala quintal",
                    "Joga na rua/vala/quintal",
                ],
                "Queimado ou enterrado": [
                    "Queimado / Enterrado",
                    "Queimado/enterrado",
                ],
                "Recolhido pela prefeitura": [
                    "Recolhido pela Prefeitura",
                ],
                "Lixeira": [
                    "Cacamba",
                    "Caçamba mais proxima",
                    "Contêiner",
                    "Contenier",
                    "Leva a lixeira lá em baixo.",
                    "Leva até a lixeira",
                    "Leva até a lixeira.",
                    "Leva na lixeira",
                    "Leva na lixeira.",
                    "Leva para a lixeira",
                    "Leva para a lixeira.",
                    "Leva pra lixeira",
                    "Levam na lixeira.",
                    "Levo até lixeira.",
                    "Lixeira",
                    "contenier",
                ],
                "Coleta seletiva": [
                    "Coleta Seletiva",
                ],
                "Outro": [
                    "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                ],
            },
            "order": [
                "Outro",
                "Queimado ou enterrado",
                "É jogado no rio ou córrego",
                "Joga na rua, vala ou quintal",
                "Lixeira",
                "Recolhido pela prefeitura",
                "Coleta seletiva",
            ],
        },
    }
    return


@app.cell(column=1)
def _():
    import random

    import altair as alt
    from great_tables import GT, md, style, loc
    import polars as pl
    import plotly.express as px
    import plotly.graph_objects as go
    from rich import print
    import numpy as np
    import marimo as mo
    return mo, np, pl, print, px


@app.cell
def _(np, pl, print, px):
    def get_assertion_map():
        """Returns the assertion map used to map answers to their respective categories."""
        from const import ASSERTION_MAP as assertion_map

        return assertion_map


    def get_descriptive_table(
        df_long_first,
        df_long_last,
        question_name,
        describe_by="race",
        mix_pp=True,
    ):
        """Get a table with the count and percentage of answers

        Args:
            df_long_first (_type_): _description_
            df_long_last (_type_): _description_
            question_name (str, optional): _description_. Defaults to "FoodManytimes".
            mix_pp (bool, optional): _description_. Defaults to True.

        Returns:
            _type_: _description_
        """
        from great_tables import GT, md, style, loc

        if mix_pp and describe_by == "race":
            df_long_first = df_long_first.with_columns(
                race=pl.when(
                    (pl.col("race") == "Parda")
                    | (pl.col("race") == "Preta")
                    | (pl.col("race") == "Indígena")
                )
                .then(pl.lit("Negros e indígenas"))
                .otherwise(pl.lit("Brancos e amarelos"))
            )
            df_long_last = df_long_last.with_columns(
                race=pl.when(
                    (pl.col("race") == "Parda")
                    | (pl.col("race") == "Preta")
                    | (pl.col("race") == "Indígena")
                )
                .then(pl.lit("Negros e indígenas"))
                .otherwise(pl.lit("Brancos e amarelos"))
            )

        conditions = (
            (pl.col("question") == question_name)
            & (pl.col(describe_by) != "NA")
            & (pl.col("answer") != "NA")
        )

        first_group_sum = (
            df_long_first.filter(conditions)
            .select(describe_by, "answer")
            .group_by(describe_by)
            .len(name="group_total_first")
        )

        last_group_sum = (
            df_long_last.filter(conditions)
            .select(describe_by, "answer")
            .group_by(describe_by)
            .len(name="group_total_last")
        )

        df_print = (
            (
                df_long_first.filter(conditions)
                .select(describe_by, "answer")
                .group_by("*")
                .len(name="count_first")
                .join(first_group_sum, on=describe_by, how="left")
                .with_columns(
                    pct_first=(pl.col("count_first") / pl.col("group_total_first")),
                )
            )
            .join(
                df_long_last.filter(conditions)
                .select(describe_by, "answer")
                .group_by("*")
                .len(name="count_last")
                .join(last_group_sum, on=describe_by, how="left")
                .with_columns(
                    pct_last=(pl.col("count_last") / pl.col("group_total_last")),
                ),
                on=[describe_by, "answer"],
                how="full",
                coalesce=True,
            )
            .with_columns(
                count_first=pl.coalesce(
                    pl.col("count_first").cast(pl.Int64), pl.lit(0).cast(pl.Int64)
                ),
                count_last=pl.coalesce(
                    pl.col("count_last").cast(pl.Int64), pl.lit(0).cast(pl.Int64)
                ),
                group_total_first=pl.coalesce(
                    pl.col("group_total_first").cast(pl.Int64), pl.lit(0).cast(pl.Int64)
                ),
                group_total_last=pl.coalesce(
                    pl.col("group_total_last").cast(pl.Int64), pl.lit(0).cast(pl.Int64)
                ),
                pct_first=pl.coalesce(pl.col("pct_first"), pl.lit(0)),
                pct_last=pl.coalesce(pl.col("pct_last"), pl.lit(0)),
            )
            .with_columns(
                delta_count=pl.col("count_last").cast(pl.Int64)
                - pl.col("count_first").cast(pl.Int64),
                delta_pct=pl.col("pct_last") - pl.col("pct_first"),
            )
            .sort("answer", describe_by)  # [TODO] Ver se é necessário ordenar por answer
        )

        answer_maps = get_answer_maps_per_question(question_name)
        answer_orders = get_answer_orders_per_question(question_name)

        assert len(answer_maps) == 1, (
            "This function only supports one answer map per question_name"
        )
        assert len(answer_orders) == 1, (
            "This function only supports one answer order per question_name"
        )

        answer_map, answer_order = answer_maps[0], answer_orders[0]

        # Correct answer names with the mapping
        df_print = df_print.with_columns(answer=pl.col("answer").replace(answer_map))

        gt_table = (
            GT(
                df_print,
                rowname_col=describe_by,
                groupname_col="answer",
            )
            .row_group_order([i for i in answer_order if i != "NA"])
            .tab_header(
                title=md(f"Respostas à pergunta {question_name}"),
                subtitle=md(
                    f"Distribuição de respostas por `{describe_by}` no tempo inicial e final"
                ),
            )
            .cols_label(
                answer=md("Resposta"),
                count_first=md("Contagem no tempo inicial"),
                count_last=md("Contagem no tempo final"),
                pct_first=md("Porcentagem no tempo inicial"),
                pct_last=md("Porcentagem no tempo final"),
                delta_count=md("Variação de contagem"),
                delta_pct=md("Variação de porcentagem"),
                group_total_first=md("Total do grupo no tempo inicial"),
                group_total_last=md("Total do grupo no tempo final"),
            )
            .fmt_number(
                columns=["count_first", "count_last", "delta_count"],
                decimals=0,
            )
            .fmt_number(
                columns=["pct_first", "pct_last"],
                decimals=2,
                scale_by=100,
                pattern="{x}%",
            )
            .fmt_number(
                columns=[
                    "delta_count",
                ],
                force_sign=True,
            )
            .fmt_number(
                columns=["delta_pct"],
                force_sign=True,
                decimals=2,
                scale_by=100,
                pattern="{x}%",
            )
            .tab_spanner(
                label=md("Tempo inicial"),
                columns=[
                    "count_first",
                    "pct_first",
                    "group_total_first",
                ],
            )
            .tab_spanner(
                label=md("Tempo final"),
                columns=[
                    "count_last",
                    "pct_last",
                    "group_total_last",
                ],
            )
            .tab_spanner(
                label=md("Variação (Final - Inicial)"),
                columns=[
                    "delta_count",
                    "delta_pct",
                ],
            )
            .tab_style(
                style=style.text(weight="bold"),
                locations=loc.body(
                    columns=[
                        "answer",
                        "pct_first",
                        "pct_last",
                        "delta_pct",
                    ]
                ),
            )
            .opt_stylize(style=1, color="blue")
            .opt_all_caps()
        )
        return gt_table


    def cramers_v(df, col1, col2, verbose=False):
        """
        Calculates Cramér's V statistic for association between two categorical columns in a Polars DataFrame.
        Cramér's V is a measure of association between two nominal variables, giving a value between 0 and +1
        (inclusive). A value of 0 indicates no association, and a value of 1 indicates perfect association.
        Parameters
        ----------
        df : pl.DataFrame
            The Polars DataFrame containing the data.
        col1 : str
            The name of the first categorical column.
        col2 : str
            The name of the second categorical column.
        verbose : bool, optional (default=False)
            If True, prints the number of valid rows used in the calculation.
        Returns
        -------
        float
            The Cramér's V statistic for the association between the two columns.
            Returns 0.0 if there is not enough data to compute the statistic.
        Notes
        -----
        - Rows with null values or "NA" in either column are excluded from the calculation.
        - The calculation is based on the chi-square statistic from the contingency table of the two columns.
        """

        # Clean data only for this pair of columns
        df_clean = df.filter(
            ~pl.any_horizontal(
                pl.col([col1, col2]).is_null() | (pl.col([col1, col2]) == "NA")
            )
        ).select(col1, col2)
        if verbose:
            print(f"{col1} X {col2} : {df_clean.height} rows")

        if df_clean.height < 2:  # Not enough data
            return 0.0

        # Create contingency table
        contingency = (
            df_clean.group_by([col1, col2])
            .len()
            .pivot(index=col1, columns=col2, values="len")
            .fill_null(0)
        )

        # Convert to numpy array for calculation
        ct = contingency.select(pl.exclude(col1)).to_numpy()

        # Calculate chi-square
        chi2 = 0
        n = ct.sum()
        row_sums = ct.sum(axis=1)
        col_sums = ct.sum(axis=0)

        for i in range(ct.shape[0]):
            for j in range(ct.shape[1]):
                expected = (row_sums[i] * col_sums[j]) / n
                if expected > 0:
                    chi2 += (ct[i, j] - expected) ** 2 / expected

        # Calculate Cramér's V
        n_rows, n_cols = ct.shape
        cramers = np.sqrt(chi2 / (n * min(n_rows - 1, n_cols - 1)))
        return cramers


    def cramers_v_scipy(df, col1, col2, verbose=False):
        """
        Calculates Cramér's V statistic for association between two categorical columns in a Polars DataFrame using scipy.stats.contingency.association.
        Parameters
        ----------
        df : pl.DataFrame
            The Polars DataFrame containing the data.
        col1 : str
            The name of the first categorical column.
        col2 : str
            The name of the second categorical column.
        verbose : bool, optional (default=False)
            If True, prints the number of valid rows used in the calculation.
        Returns
        -------
        float
            The Cramér's V statistic for the association between the two columns.
            Returns 0.0 if there is not enough data to compute the statistic.
        """
        from scipy.stats.contingency import association

        df_clean = df.filter(
            ~pl.any_horizontal(
                pl.col([col1, col2]).is_null() | (pl.col([col1, col2]) == "NA")
            )
        ).select(col1, col2)
        if verbose:
            print(f"{col1} X {col2} : {df_clean.height} rows")

        if df_clean.height < 2:
            return 0.0

        contingency = (
            df_clean.group_by([col1, col2])
            .len()
            .pivot(index=col1, columns=col2, values="len")
            .fill_null(0)
        )
        ct = contingency.select(pl.exclude(col1)).to_numpy()

        return association(ct, method="cramer")


    def get_vars_IGF():
        return [
            "Access",
            "BankAccount",
            "Bathroom",
            "CEP",
            "CommFacilities",
            "CulturalEvent",
            "Documents",
            "Eletricity",
            "Floor",
            "FoodManytimes",
            "Garbage",
            "HealthGenKidsNames",
            "HealthGenNames",
            "HousingProblems",
            "Income",
            "IncomeWorkS3",
            "Internet",
            "Roof",
            "SchoolCurrent",
            "SchoolLast",
            "SchoolLiteracy",
            "SchoolMathLit",
            "Sewer",
            "Walls",
            "Water",
            "WaterFrequency",
        ]


    def plot_variables(
        df_long,
        question_names: list,
        seggregate_favela=False,
        orientation="v",
        palette=px.colors.qualitative.Pastel,
        percentage=False,
        color_col="time",
        compare_by_col=None,
        unique_per_family=False,
        filter_by_order=False,
        title=None,
        subtitle=None,
        verbose=False,
        max_y=None,
    ):
        """Plots the variables from a list of question names, each in a bar chart.

        Args:
            df_long (pl.DataFrame): The polars DataFrame in long format (one row questionnaire entry, having columns 'question', 'answer', 'time', 'FavelaID').
            question_names (list): The list of variable names to plot.
            seggregate_favela (bool, optional): If the bar plot should segment the results by favela. Defaults to False.
            orientation (str, optional): Orientation of the bars in the bar plot, "h" for horizontal, "v" for vertical. Defaults to "v".
            palette (list(str), optional): Color palette as defined in the plotly express lib. Defaults to px.colors.qualitative.Pastel.
            percentage (bool, optional): Wether the plots should show percentages (of the 'time') or absolute numbers of answers. Defaults to False.
        """
        # color_col = "time"

        for question_name in question_names:
            agg_cols = [color_col, "answer"]
            if seggregate_favela:
                agg_cols.append("FavelaID")
            if compare_by_col is not None:
                agg_cols.append(compare_by_col)

            # Initializing columns for the hover data and subtitle variable
            hover_cols = ["len", "percentage", "percentage_wo_na"]
            subtitle = subtitle if subtitle else ""

            # Formatting the hover data
            hover_dict = {k: True for k in agg_cols + hover_cols}
            hover_dict["percentage"] = ":.2f"
            hover_dict["percentage_wo_na"] = ":.2f"

            # Getting answer maps and orders for the question
            answer_maps = get_answer_maps_per_question(question_name)
            answer_orders = get_answer_orders_per_question(question_name)

            assert len(answer_maps) == len(answer_orders), (
                f"Number of answer maps ({len(answer_maps)}) does not match number of answer orders ({len(answer_orders)}) for question '{question_name}'"
            )

            for answer_map, answer_order in zip(answer_maps, answer_orders):
                # Correct answer names with the mapping
                df_plot = df_long.with_columns(
                    answer=pl.when(pl.col("question") == question_name)
                    .then(pl.col("answer").replace(answer_map))
                    .otherwise(pl.col("answer"))
                )

                conditions = pl.col("question") == question_name
                if answer_order != ["NA"] and filter_by_order:
                    if verbose:
                        print("Removing from dataframe:")
                        print(
                            df_plot.filter(
                                conditions & pl.col("answer").is_in(answer_order).not_()
                            )
                            .select(agg_cols)
                            .group_by(agg_cols)
                            .len()
                        )

                    conditions &= pl.col("answer").is_in(answer_order)

                df_plot = (
                    df_plot.filter(conditions).select(agg_cols).group_by(agg_cols).len()
                )

                # [TODO] Solução WIP para agrupar a uma resposta por família.
                # [TODO] Tem que ter assertions pra garantir que não estamos agrupando quando não faz sentido (família com respostas diferentes para a mesma pergunta). Add mensagems claras desse erro.
                if unique_per_family:
                    # If unique_per_family is True, we need to filter the dataframe to have only one row per family
                    df_plot = (
                        df_plot.group_by("id_family_datalake", "time").agg(
                            pl.col("answer").first()
                        )
                        # .rename({"answer": "answer"})
                    )

                df_pct = df_plot.with_columns(
                    percentage=(pl.col("len") / pl.col("len").sum().over("time") * 100)
                )

                df_pct_wo_na = df_plot.filter(pl.col("answer") != "NA").with_columns(
                    percentage_wo_na=(
                        pl.col("len") / pl.col("len").sum().over("time") * 100
                    )
                )

                df_plot = (
                    df_plot.join(df_pct_wo_na, on=agg_cols, how="left", suffix="_wo_na")
                    .with_columns(
                        percentage_wo_na=pl.when(pl.col("percentage_wo_na").is_null())
                        .then(pl.lit("NÃO INCLUÍDO"))
                        .otherwise(pl.col("percentage_wo_na"))
                    )
                    .join(df_pct, on=agg_cols, how="left", suffix="_na")
                )

                amount_col = "len"

                if percentage:
                    amount_col = "percentage"

                if orientation == "v":
                    kwargs = dict(
                        x="answer", y=amount_col, orientation="v", facet_row=compare_by_col
                    )
                else:
                    kwargs = dict(
                        x=amount_col, y="answer", orientation="h", facet_row=compare_by_col
                    )
                if compare_by_col is not None:
                    kwargs["height"] = 1000
                    # kwargs["facet_row_spacing"] = 0.05
                    # kwargs["facet_col_spacing"] = 0.05
                    kwargs["facet_row"] = compare_by_col

                if max_y is not None:
                    kwargs["range_y"] = [0, max_y] if orientation == "v" else [0, None]
                    kwargs["range_x"] = [0, max_y] if orientation == "h" else [0, None]

                fig = px.bar(
                    df_plot,
                    **kwargs,
                    color=color_col,
                    # height=1000,
                    color_discrete_sequence=palette,  # [NOTE] Ver https://plotly.com/python/discrete-color/
                    barmode="group",
                    title=title if title else f"Respostas à pergunta {question_name}",
                    # title=f"{questions_dict.get(question_name)}", # [TODO] Add function to get the full question from the question name
                    subtitle=subtitle,
                    hover_data=hover_dict,
                    hover_name="answer",
                    labels=dict(
                        answer="Resposta",
                        len="Núm. de respostas"
                        if question_name  # Questões com múltiplas asserções
                        in [
                            "BathroomQualit",
                            "HousingProblems",
                            "CommFacilities",
                            "Garbage",
                            "HealthGenKidsNames",
                            "HealthGenNames",
                            "Internet",
                            "Documents",
                            "IncomeDesc",
                            "JobSatisfaction",
                        ]
                        else "Núm. de famílias",
                        time="Período",
                        percentage="% no período (com NAs)",
                        percentage_wo_na="% no período (excluindo NAs)",
                        FavelaID="Favela",
                    ),
                    category_orders={
                        "time": ["FIRST", "LAST"],
                        "drug_addiction": ["Não", "Talvez", "Sim", "NA"],
                        # "time": ["0", "1", "2", "3", "FIRST", "LAST"],
                        "answer": answer_order,
                    },
                )
                fig.update_layout(margin=dict(t=100, l=100, r=100, b=100))
                fig.show()

                if verbose:
                    print("\nPlotting question:", question_name)
                    print("\nAnswer order:", answer_order)
                    print("\nAnswer map:", answer_map)
                    print(
                        "\nOptions on plot:",
                        sorted(df_plot.select("answer").unique().to_series().to_list()),
                    )
        return


    def get_answer_map_per_question(question_name):
        assertion_map = get_assertion_map()
        new_to_list_of_old = assertion_map.get(question_name, {}).get("map", {})
        old_to_new = {
            value: key for key, values in new_to_list_of_old.items() for value in values
        }
        return old_to_new


    def get_answer_maps_per_question(question_name):
        # Handles assertion_map entries that are a dict or a list of dicts
        assertion_map = get_assertion_map()
        entry = assertion_map.get(question_name, {})
        answer_maps = []

        if not isinstance(entry, list):
            entry = [entry]

        for e in entry:
            if isinstance(e, dict):
                if "map" in e:
                    new_to_list_of_old = e["map"]
                    old_to_new = {
                        v: k for k, vals in new_to_list_of_old.items() for v in vals
                    }
                    answer_maps.append(old_to_new)
                else:
                    # If no map is defined, return an empty map
                    answer_maps.append({})

        return answer_maps


    def get_answer_order_per_question(question_name):
        assertion_map = get_assertion_map()
        if question_name.startswith("Categoria"):
            return [
                "NA",
                "E1",
                "E2",
                "P1",
                "P2",
                "D",
            ]

        return assertion_map.get(question_name, {}).get("order", ["NA"])


    def get_answer_orders_per_question(question_name):
        assertion_map = get_assertion_map()
        ORDER_KEY = "order"

        if question_name.startswith("Categoria"):
            return [
                [
                    "NA",
                    "E1",
                    "E2",
                    "P1",
                    "P2",
                    "D",
                ]
            ]

        entry = assertion_map.get(question_name, {})
        answer_orders = []

        if not isinstance(entry, list):
            entry = [entry]

        for e in entry:
            if isinstance(e, dict):
                if ORDER_KEY in e:
                    answer_orders.append(e[ORDER_KEY])
                else:
                    answer_orders.append(["NA"])

        return answer_orders


    def enrich_first_and_last_time(df_long):
        """Enriches the dataframe with the first and last time for each family.

        Args:
            df_long (pl.DataFrame): The polars DataFrame in long format (one row questionnaire entry, having columns 'question', 'answer', 'time', 'FavelaID').

        Returns:
            pl.DataFrame: The dataframe enriched with columns 'time_first' and 'time_last', with the first and last collection times for each family.
        """
        # Gets the first time for each family
        df_first = (
            df_long.filter(
                (pl.col("question") == "CategoriaIGF") & (pl.col("answer") != "NA")
            )
            .select("id_family_datalake", "column", "time")
            .sort("id_family_datalake", "time")
            .group_by("id_family_datalake")
            .first()
            .select("id_family_datalake", "time")
        )

        # Gets the last time for each family
        df_last = (
            df_long.filter(
                (pl.col("question") == "CategoriaIGF") & (pl.col("answer") != "NA")
            )
            .select("id_family_datalake", "column", "time")
            .sort("id_family_datalake", "time")
            .group_by("id_family_datalake")
            .last()
            .select("id_family_datalake", "time")
        )

        # Join the first and last times to the main dataframe
        df_long_periods = df_long.join(
            df_first, on="id_family_datalake", how="left", suffix="_first"
        )

        # Join the last time to the main dataframe
        df_long_periods = df_long_periods.join(
            df_last, on="id_family_datalake", how="left", suffix="_last"
        )

        # Remove families that only have one time difference between the first and last time (this filters out 57 families, leaving only the 616 remaining families)
        df_long_periods = df_long_periods.filter(
            pl.col("time_last").cast(pl.Int8) - pl.col("time_first").cast(pl.Int8) > 1
        )

        return df_long_periods


    def get_col_dict():
        """Returns a dictionary with the column names to be used in the dataframe.
        The keys are the original column names and the values are the new column names.
        """
        vars_T1_3 = [
            "HealthGenNames",
            "HealthGenKidsNames",
            "M_ATI_Walls",
            "M_ATI_Roof",
            "M_ATI_Floor",
            "M_ATI_Water",
            "M_ATI_WaterFrequency",
            "M_ATI_Eletricity",
            "M_ATI_Sewer",
            "M_ATI_Bathroom",
            "BathroomQualit",
            "CommFacilities",
            "HousingProblems",
            "H_ATI_FoodManytimes",
            "Internet",
            "Documents",
            "C_ATI_CEP",
            "E_ATI_SchoolLiteracy",
            "E_ATI_SchoolMathLit",
            "E_ATI_SchoolLast",
            "E_ATI_SchoolCurrent",
            "E_ATI_KidsSchool2N",
            "ES_ATI_Access",
            "ES_ATI_CulturalEvent",
            "R_ATI_Income",
            "R_ATI_IncomeWorkS3",
            "R_ATI_BankAccount",
            "Garbage",
            "HousingProblems",
            "IncomeDesc",
            "JobSatisfaction",
        ]
        complementary_vars = {}
        vars_T1_3_main = [var.split("_")[-1] if "_" in var else var for var in vars_T1_3]

        dic_T1_3_list = [
            {f"{k}_T{t}": f"{v}_T{t}" for k, v in zip(vars_T1_3, vars_T1_3_main)}
            for t in range(1, 4)
        ]
        dic_T1_3 = (
            dic_T1_3_list[0] | dic_T1_3_list[1] | dic_T1_3_list[2] | complementary_vars
        )

        col_dict = (
            {
                "Categoria_IGF_T0": "CategoriaIGF_T0",
                "Categoria_Income_T0": "CategoriaIncome_T0",
                "Categoria_Environment_T0": "CategoriaEnvironment_T0",
                "Categoria_Housing_T0": "CategoriaHousing_T0",
                "Categoria_Schooling_T0": "CategoriaSchooling_T0",
                "Categoria_Health_T0": "CategoriaHealth_T0",
                "Categoria_WomanAutonomy_T0": "CategoriaWomanAutonomy_T0",
                "Categoria_Citizenship_T0": "CategoriaCitizenship_T0",
                "Categoria_FirstInfancy_T0": "CategoriaFirstInfancy_T0",
                "Categoria_Culture_T0": "CategoriaCulture_T0",
                "Categoria_IGF_T1": "CategoriaIGF_T1",
                "Categoria_Income_T1": "CategoriaIncome_T1",
                "Categoria_Housing_T1": "CategoriaHousing_T1",
                "Categoria_Schooling_T1": "CategoriaSchooling_T1",
                "Categoria_Health_T1": "CategoriaHealth_T1",
                "Categoria_WomanAutonomy_T1": "CategoriaWomanAutonomy_T1",
                "Categoria_Citizenship_T1": "CategoriaCitizenship_T1",
                "Categoria_FirstInfancy_T1": "CategoriaFirstInfancy_T1",
                "Categoria_Culture_T1": "CategoriaCulture_T1",
                "Categoria_Environment_T1": "CategoriaEnvironment_T1",
                "Categoria_IGF_T2": "CategoriaIGF_T2",
                "Categoria_Income_T2": "CategoriaIncome_T2",
                "Categoria_Housing_T2": "CategoriaHousing_T2",
                "Categoria_Schooling_T2": "CategoriaSchooling_T2",
                "Categoria_Health_T2": "CategoriaHealth_T2",
                "Categoria_WomanAutonomy_T2": "CategoriaWomanAutonomy_T2",
                "Categoria_Citizenship_T2": "CategoriaCitizenship_T2",
                "Categoria_FirstInfancy_T2": "CategoriaFirstInfancy_T2",
                "Categoria_Culture_T2": "CategoriaCulture_T2",
                "Categoria_Environment_T2": "CategoriaEnvironment_T2",
                "Categoria_IGF_T3": "CategoriaIGF_T3",
                "Categoria_Income_T3": "CategoriaIncome_T3",
                "Categoria_Housing_T3": "CategoriaHousing_T3",
                "Categoria_Schooling_T3": "CategoriaSchooling_T3",
                "Categoria_Health_T3": "CategoriaHealth_T3",
                "Categoria_WomanAutonomy_T3": "CategoriaWomanAutonomy_T3",
                "Categoria_Citizenship_T3": "CategoriaCitizenship_T3",
                "Categoria_FirstInfancy_T3": "CategoriaFirstInfancy_T3",
                "Categoria_Culture_T3": "CategoriaCulture_T3",
                "Categoria_Environment_T3": "CategoriaEnvironment_T3",
                #
                "IGF_SimpleAverage_T0": "AverageIGF_T0",
                "Factor_Income_SimpleAverage_N_T0": "AverageIncome_T0",
                "Factor_Housing_SimpleAverage_N_T0": "AverageHousing_T0",
                "Factor_Schooling_SimpleAverage_Threshold_N_T0": "AverageSchooling_T0",
                "Factor_Health_SimpleAverage_N_T0": "AverageHealth_T0",
                "Factor_WomanAutonomy_SimpleAverage_N_T0": "AverageWomanAutonomy_T0",
                "Factor_Citizenship_SimpleAverage_N_T0": "AverageCitizenship_T0",
                "Factor_FirstInfancy_SimpleAverage_Threshold_N_T0": "AverageFirstInfancy_T0",
                "Factor_Culture_SimpleAverage_N_T0": "AverageCulture_T0",
                "Factor_Environment_SimpleAverage_N_T0": "AverageEnvironment_T0",
                "IGF_SimpleAverage_T1": "AverageIGF_T1",
                "Factor_Income_SimpleAverage_N_T1": "AverageIncome_T1",
                "Factor_Housing_SimpleAverage_N_T1": "AverageHousing_T1",
                "Factor_Schooling_SimpleAverage_Threshold_N_T1": "AverageSchooling_T1",
                "Factor_Health_SimpleAverage_N_T1": "AverageHealth_T1",
                "Factor_WomanAutonomy_SimpleAverage_N_T1": "AverageWomanAutonomy_T1",
                "Factor_Citizenship_SimpleAverage_N_T1": "AverageCitizenship_T1",
                "Factor_FirstInfancy_SimpleAverage_Threshold_N_T1": "AverageFirstInfancy_T1",
                "Factor_Culture_SimpleAverage_N_T1": "AverageCulture_T1",
                "Factor_Environment_SimpleAverage_N_T1": "AverageEnvironment_T1",
                "IGF_SimpleAverage_T2": "AverageIGF_T2",
                "Factor_Income_SimpleAverage_N_T2": "AverageIncome_T2",
                "Factor_Housing_SimpleAverage_N_T2": "AverageHousing_T2",
                "Factor_Schooling_SimpleAverage_Threshold_N_T2": "AverageSchooling_T2",
                "Factor_Health_SimpleAverage_N_T2": "AverageHealth_T2",
                "Factor_WomanAutonomy_SimpleAverage_N_T2": "AverageWomanAutonomy_T2",
                "Factor_Citizenship_SimpleAverage_N_T2": "AverageCitizenship_T2",
                "Factor_FirstInfancy_SimpleAverage_Threshold_N_T2": "AverageFirstInfancy_T2",
                "Factor_Culture_SimpleAverage_N_T2": "AverageCulture_T2",
                "Factor_Environment_SimpleAverage_N_T2": "AverageEnvironment_T2",
                "IGF_SimpleAverage_T3": "AverageIGF_T3",
                "Factor_Income_SimpleAverage_N_T3": "AverageIncome_T3",
                "Factor_Housing_SimpleAverage_N_T3": "AverageHousing_T3",
                "Factor_Schooling_SimpleAverage_Threshold_N_T3": "AverageSchooling_T3",
                "Factor_Health_SimpleAverage_N_T3": "AverageHealth_T3",
                "Factor_WomanAutonomy_SimpleAverage_N_T3": "AverageWomanAutonomy_T3",
                "Factor_Citizenship_SimpleAverage_N_T3": "AverageCitizenship_T3",
                "Factor_FirstInfancy_SimpleAverage_Threshold_N_T3": "AverageFirstInfancy_T3",
                "Factor_Culture_SimpleAverage_N_T3": "AverageCulture_T3",
                "Factor_Environment_SimpleAverage_N_T3": "AverageEnvironment_T3",
                # ----------------
                # ÍNDICE T0
                "M_DC_Walls_T0": "Walls_T0",
                "M_DC_Roof_T0": "Roof_T0",
                "M_DC_Floor_T0": "Floor_T0",
                # ----------------
                "M_DC_Water_T0": "Water_T0",
                "M_DC_WaterFrequency_T0": "WaterFrequency_T0",
                "M_DC_Eletricity_T0": "Eletricity_T0",
                "M_DC_Sewer_T0": "Sewer_T0",
                "M_DC_Bathroom_T0": "Bathroom_T0",
                # ----------------
                # O QUE USAR AQUI? -> Vamos consolidar BathroomQualit em cada tempo de uma forma que a Carol vai ver [NOTE]
                # [NOTE] Variáveis que vamos consolidar contando o número de asserções de um subconjunto
                # [TODO] Fazer para
                # ---
                # "M_DC_BathroomQualit_1_T0": "BathroomQualit_T0",
                # "M_DC_BathroomQualit_2_T0": "BathroomQualit_T0",
                # "M_DC_BathroomQualit_3_T0": "BathroomQualit_T0",
                # "M_DC_BathroomQualit_4_T0": "BathroomQualit_T0",
                # "M_DC_BathroomQualit_5_T0": "BathroomQualit_T0",
                # "M_DC_BathroomQualit_6_T0": "BathroomQualit_T0",
                # "M_DC_BathroomQualit_7_T0": "BathroomQualit_T0",
                # "M_DC_BathroomQualit_8_T0": "BathroomQualit_T0",
                # ---
                # MAIS MELHOR
                "CommFacilities_T0": "CommFacilities_T0",
                # ---
                # MENOS MELHOR
                "HousingProblems_T0": "HousingProblems_T0",
                "HealthGenNames_T0": "HealthGenNames_T0",  # [TODO] Aqui também fazer visualização da incidência de cada doença no Tinicial e Tfinal
                "HealthGenKidsNames_T0": "HealthGenKidsNames_T0",  # [TODO] Aqui também fazer visualização da incidência de cada doença no Tinicial e Tfinal
                # ----------------
                "H_DC_FoodManytimes_T0": "FoodManytimes_T0",
                "Internet_T0": "Internet_T0",
                "Documents_T0": "Documents_T0",
                "C_DC_CEP_T0": "CEP_T0",
                "CH_E_DC_SchoolLiteracy_T0": "SchoolLiteracy_T0",
                "CH_E_DC_SchoolMathLit_T0": "SchoolMathLit_T0",
                "E_DI_SchoolLast_T0": "SchoolLast_T0",
                "E_DI_SchoolCurrent_T0": "SchoolCurrent_T0",  # [TODO] Juntar respostas Nao e Não
                "ES_DC_Access_T0": "Access_T0",
                "ES_DC_CulturalEvent_T0": "CulturalEvent_T0",
                # "IB_HowManyPHHH_T0": "HowManyPHHH_T0",
                "R_DC_Income_T0": "Income_T0",
                "R_DI_IncomeWorkS3_T0": "IncomeWorkS3_T0",
                "R_DI_BankAccount_T0": "BankAccount_T0",
                "Garbage_T0": "Garbage_T0",
                "DI_IncomeDesc_T0": "IncomeDesc_T0",
                "IncomeDesc_T0": "IncomeDesc_T0",
                "DI_JobSatisfaction_T0": "JobSatisfaction_T0",
                "JobSatisfaction_T0": "JobSatisfaction_T0",
            }
            | dic_T1_3
        )

        return col_dict


    mais_melhor = {
        "BathroomQualit": [
            "Box ou cortina que fecha o chuveiro"
            "Chuveiro com água quente"
            "Parede de azulejo"
            "Piso de azulejo"
            "Porta externa que fecha o banheiro"
            "Privada com tampa"
        ],
        "CommFacilities": [
            "Agua Encanada",
            "Água encanada",
            "Boas condições das ruas, vielas ou escadas que dão acesso à comunidade",
            "Creche Publica",
            "Creche Pública",
            "Escola Publica",
            "Escola Pública",
            "Esgoto",
            "Acesso à rede de esgoto",
            "Espaços para reuniões comunitárias",
            "Hospital Publico",
            "Hospital Público",
            "Iluminacao Publica",
            "Iluminação Pública",
            "Opcoes de Lazer",
            "Opções de lazer",
            "Pavimentação das ruas e vielas da comunidade",
            "Posto de Saude",
            "Posto de Saúde",
            "Transporte Publico",
            "Transporte Público",
            "Coleta de Lixo",
        ],
    }

    menos_melhor = {
        "HousingProblems": [
            "Alagamento Inundacao",
            "Chuva Goteiras",
            "Cozinha Com Lenha",
            "Cupim",
            "Deslizamento",
            "Desmoronamento",
            "Incendio",
            "Outro",
            "Ratos Baratas Animais Indesejados",
            "Solapamento",
            "Umidade Mofo",
        ],
        "HealthGenNames": ["TODAS"],  # [TODO]
        "HealthGenKidsNames": ["TODAS"],  # [TODO]
    }

    # %%
    # LISTA DE VARIÁVEIS COM MÚLTIPLAS ASSERÇÕES POR RESPOSTA

    # - "BathroomQualit" # MAIS MELHOR
    # - "HousingProblems" # MENOS MELHOR
    # - "CommFacilities" MAIS MELHOR
    # - "Garbage"  # SÓ LIMPAR AS RESPOSTAS (é uma asserção)
    # ----------- DOIS GRAFICOS TAMBEM ((1) contagem geral e (2) incidência)
    # - "HealthGenKidsNames"
    # - "HealthGenNames"
    # -----------
    # - "Internet"  # DOIS GRÁFICOS:
    # 1) Não tenho acesso à internet e Tenho acesso à internet (Todas as outras alternativas)
    # 2) Quantas vezes cada alternativa foi selecionada no tempo inicial e final
    # -----------
    # - "Documents"  # DOIS GRÁFICOS TAMBÉM, IGUAL INTERNET
    # [TODO]
    # "Não tenho nenhum documento" # [TODO] Não usar a coluna, criar uma nova para os casos que sejam só NA;NA..NA
    # -----------


    # %%


    def get_df_long(df):
        col_dict = get_col_dict()

        # profile_cols are columns that are only answered once and describe the profile of the family. They should become columns in the long format.
        profile_cols = {
            "Drogadicao": "Drogadicao",
            "Alcoolismo": "Alcoolismo",
            "Violencia_Mulher": "ViolenciaMulher",
            "Violencia_Criança": "ViolenciaCrianca",
            "CH_IB_Race_T0": "Race",
            "CH_IB_Gender_T0": "Gender",
        }

        race_categories = {
            "Parda": "Parda",
            "Preta": "Preta",
            "Branca": "Branca",
            "Indigena": "Indígena",
            "Amarela": "Amarela",
            "Amarela (Asiática)": "Amarela",
            "NA": "NA",
            "Não sabe/Não respondeu": "NA",
            "Não sabe": "NA",
        }
        gender_categories = {
            "Mulher cisgênero": "Mulher cis",
            "Homem cisgênero": "Homem cis",
            "Mulher transgênero": "Mulher trans",
            "Homem transgênero": "Homem trans",
            "Não binário": "Não binário",
            "NA": "NA",
            "Outro": "NA",
            "Prefiro não responder": "NA",
        }
        generic_categories = {
            "sim": "Sim",
            "nao": "Não",
            "NA": "NA",
            "talvez": "Sim",
            # "talvez": "Talvez",
        }

        df_long = (
            df.unpivot(
                index=["id_family_datalake", "FavelaID"] + list(profile_cols.keys()),
                variable_name="original_column",
                value_name="answer",
            )
            .join(  # Join with the column dictionary to get the new column names
                pl.DataFrame(
                    {
                        "original_column": list(col_dict.keys()),
                        "column": list(col_dict.values()),
                    }
                ),
                on="original_column",
                how="left",
            )
            # Joins with profile dataframes to get the profile information
            .join(
                pl.DataFrame(
                    {
                        "CH_IB_Race_T0": list(race_categories.keys()),
                        "race": list(race_categories.values()),
                    }
                ),
                on="CH_IB_Race_T0",
                how="left",
            )
            .join(
                pl.DataFrame(
                    {
                        "CH_IB_Gender_T0": list(gender_categories.keys()),
                        "gender": list(gender_categories.values()),
                    }
                ),
                on="CH_IB_Gender_T0",
                how="left",
            )
            .join(
                pl.DataFrame(
                    {
                        "Drogadicao": list(generic_categories.keys()),
                        "drug_addiction": list(generic_categories.values()),
                    }
                ),
                on="Drogadicao",
                how="left",
            )
            .join(
                pl.DataFrame(
                    {
                        "Alcoolismo": list(generic_categories.keys()),
                        "alcoholism": list(generic_categories.values()),
                    }
                ),
                on="Alcoolismo",
                how="left",
            )
            .join(
                pl.DataFrame(
                    {
                        "Violencia_Mulher": list(generic_categories.keys()),
                        "violence_women": list(generic_categories.values()),
                    }
                ),
                on="Violencia_Mulher",
                how="left",
            )
            .join(
                pl.DataFrame(
                    {
                        "Violencia_Criança": list(generic_categories.keys()),
                        "violence_children": list(generic_categories.values()),
                    }
                ),
                on="Violencia_Criança",
                how="left",
            )
            .with_columns(
                time=pl.col("column").str.extract(r"_T(\d)$", 1),
                question=pl.col("column").str.extract(r"(.*_)*(.*)_T(\d)$", 2),
            )
            # Padronizar o separados em questions com múltiplas asserções
            .with_columns(
                answer=pl.when(
                    (pl.col("question") == "HealthGenNames")
                    | (pl.col("question") == "HealthGenKidsNames")
                )
                .then(pl.col("answer").str.replace_all(",", ";"))
                .otherwise(pl.col("answer"))
            )
            .with_columns(
                FavelaID=pl.when(pl.col("FavelaID") == "Boca do Sapo (São Paulo)")
                .then(pl.lit("Favela dos Sonhos (São Paulo)"))
                .otherwise(pl.col("FavelaID"))
            )
            # .with_columns([pl.col(k).alias(v) for k, v in profile_cols.items()])
            .select(
                [
                    "id_family_datalake",
                    "FavelaID",
                    "time",
                    "question",
                    "answer",
                    "column",
                    "original_column",
                ]
                + [
                    "race",
                    "gender",
                    "drug_addiction",
                    "alcoholism",
                    "violence_women",
                    "violence_children",
                ]
            )
            .filter(
                pl.col("column").is_in(list(col_dict.values()))
            )  # Mantém apenas as colunas de interesse
        )

        # Separa as asserções no caso de haver múltiplas asserções na resposta
        df_long = pl.concat(
            [
                # [TODO] Verificar nas perguntas multi-asserções o que fazer com quem não respondeu nenhuma (ex.: "NA;NA;NA"). Algo como "Nenhuma opção". Importante para os gráficos em que vamos contar o número de asserções positivas ou negativas.
                df_long.filter((pl.col.answer != "NA"))
                .with_columns(answer=pl.col("answer").str.split(";"))
                .explode("answer")
                .filter(pl.col("answer") != "NA"),
                # Concatena com as respostas que são "NA;NA;...;NA" (nenhuma opção)
                df_long.filter(pl.col("answer").str.contains(r"^(NA;)+NA$")),
                # Concatena com as respostas que são "NA" (não respondidas)
                df_long.filter((pl.col.answer == "NA")),
            ]
        )

        df_long = df_long.with_columns(
            pl.when(pl.col("answer").str.contains(r"^(NA;)+NA$"))
            .then(pl.lit("Nenhuma opção"))
            .otherwise(pl.col("answer"))
            .alias("answer")
        )

        return df_long


    def correct_answer_names(df_long, question_name, answer_map):
        """Corrects the answer names in the dataframe to match the expected values."""

        df_long = df_long.with_columns(
            answer=pl.when(pl.col("question") == question_name)
            .then(pl.col("answer").replace(answer_map))
            .otherwise(pl.col("answer"))
        )

        return df_long


    def find_in_columns_from_df_long(df_long, search_term, col_name="column"):
        filtered_list = [
            item
            for item in df_long.select(col_name).unique().to_series().to_list()
            if search_term in item
        ]
        print(filtered_list)

    return (get_df_long,)


if __name__ == "__main__":
    app.run()

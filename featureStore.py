import psycopg2

class FeatureStore:
    def __init__(self, host='localhost', database='featureStore', user='postgres', password='gutxi5ip4'):
        self.connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )

    def register(self, nome, descricao, origens):
        try:
            with self.connection.cursor() as cursor:
                # Verifica se a feature já existe na tabela Features
                cursor.execute(f"SELECT COUNT(*) FROM Features WHERE nome = '{nome}';")
                feature_count = cursor.fetchone()[0]
                if feature_count == 0:
                    # Insere a feature na tabela Features
                    cursor.execute(f"INSERT INTO Features (nome, descricao) VALUES ('{nome}', '{descricao}');")
                else:
                    # Atualiza a descrição da feature (depende de definicao de regra)
                    cursor.execute(f"UPDATE Features SET descricao = '{descricao}' WHERE nome = '{nome}';")
                # Insere as origens na tabela Origens e as colunas na tabela Colunas
                for origem in origens:
                    nome_tabela = origem['nome_tabela']
                    coluna_data = origem['coluna_data']
                    colunas = origem['colunas']
                    # Verifica se a origem já existe na tabela Origens
                    cursor.execute(f"SELECT COUNT(*) FROM Origens WHERE nome_tabela = '{nome_tabela}';")
                    origem_count = cursor.fetchone()[0]
                    if origem_count == 0:
                        # Insere a origem na tabela Origens
                        cursor.execute(f"INSERT INTO Origens (nome_tabela, coluna_data) VALUES ('{nome_tabela}', '{coluna_data}');")
                        # Insere as colunas na tabela Colunas
                        for coluna in colunas:
                            cursor.execute(f"INSERT INTO Colunas (origem_nome_tabela, nome) VALUES ('{nome_tabela}', '{coluna}');")
                    else:
                        for coluna in colunas:
                            cursor.execute(f"SELECT COUNT(*) FROM Colunas WHERE origem_nome_tabela = '{nome_tabela}' AND nome = '{coluna}';")
                            coluna_count = cursor.fetchone()[0]
                            if coluna_count == 0:
                                cursor.execute(f"INSERT INTO Colunas (origem_nome_tabela, nome) VALUES ('{nome_tabela}', '{coluna}');")

                    for coluna in colunas:
                        cursor.execute(f"SELECT id FROM Colunas WHERE origem_nome_tabela='{nome_tabela}' AND nome='{coluna}'")
                        col_id = cursor.fetchone()[0]
                        # Falta verificar se essa relacao ja existe no banco
                        cursor.execute(f"INSERT INTO FeatureColunas (feature_nome, coluna_id) VALUES ('{nome}', '{col_id}');")

            self.connection.commit()
        except Exception as e:
            print(e)
            self.connection.rollback()

    def load_features(self, features):
        with self.connection.cursor() as cursor:
            features_list = []
            for feature in features:
                cursor.execute(f"""
                    SELECT json_build_object('nome_feature', a.nome, 'descricao', a.descricao,
                        'origem', json_agg(a.origem))
                    FROM
                    (SELECT f.nome, f.descricao, 
                        json_build_object('nome_tabela', o.nome_tabela, 'coluna_data', o.coluna_data, 'colunas', json_agg(col.nome)) as Origem
                    FROM Features f
                    JOIN FeatureColunas fc ON f.nome = fc.feature_nome
                    JOIN Colunas col ON fc.coluna_id = col.id
                    JOIN Origens o ON col.origem_nome_tabela = o.nome_tabela
                    WHERE f.nome = '{feature}'
                    GROUP BY f.nome, f.descricao, o.nome_tabela
                    ) a
                    GROUP BY a.nome, a.descricao;
                """)
                
                result = cursor.fetchall()

                features_list += result

        return features_list

if __name__ == "__main__":
    fs = FeatureStore()
    fs.register('teste6', 'variavel de teste', [{"nome_tabela": 'tabelaA', "coluna_data":'colunaD', 'colunas':['colunaJ', 'colunaY', 'colunaD']}, {"nome_tabela": 'tabelaB', "coluna_data":'colunaD', 'colunas':['colunaA', 'colunaB', 'colunaC']}])
    dicion = fs.load_features(['teste6', 'teste2'])
    print(dicion[0])

### TABELAS ###
# ORIGENS
    # PK = nome_tabela
# FEATURES
    # PK = nome_feature
# COLUNAS
    # PK = id(AutoInc)
    # FK = nome_tabela
# FEATURESCOLUNAS (BRIDGE)
    # PK|FK = id(COLUNAS), nome_feature(FEATURES)
        # CREATE TABLE FeatureColunas (
        # feature_nome VARCHAR(100) REFERENCES Features(nome),
        # coluna_id INTEGER REFERENCES Colunas(id),
        # PRIMARY KEY (feature_nome, coluna_id));
from pyspark.sql import SparkSession
from pyspark.ml.classification import (GBTClassifier, MultilayerPerceptronClassifier, 
                                       LogisticRegression, LinearSVC)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col, lit

import os
from pyspark.ml import PipelineModel

class UpliftModelTrainer:
    def __init__(self, model_type):
        self.model_type = model_type
        self.model = self._initialize_model()
        self.pipeline_model = None

    def _initialize_model(self):
        """
        Inicializa o modelo escolhido com configurações padrão.
        """
        if self.model_type == "GBT":
            return GBTClassifier(featuresCol="features", labelCol="label", weightCol="classWeight", 
                                 maxIter=100, maxDepth=7, subsamplingRate=0.8)
        elif self.model_type == "MLP":
            return MultilayerPerceptronClassifier(featuresCol="features", labelCol="label", 
                                                  layers=[10, 5, 2], stepSize=0.03, maxIter=200)
        elif self.model_type == "LogisticRegression":
            return LogisticRegression(featuresCol="features", labelCol="label", weightCol="classWeight", 
                                      maxIter=100, regParam=0.1, elasticNetParam=0.5)
        elif self.model_type == "LinearSVC":
            return LinearSVC(featuresCol="features", labelCol="label", weightCol="classWeight", 
                             maxIter=100, regParam=0.01)
        else:
            raise ValueError(f"Modelo {self.model_type} não suportado.")

    def train(self, train_data):
        """
        Treina o modelo com os dados fornecidos.
        """
        self.pipeline_model = self.model.fit(train_data)
        return self.pipeline_model

    def transform(self, data):
        """
        Faz previsões utilizando o modelo treinado.
        """
        if not self.pipeline_model:
            raise ValueError("O modelo ainda não foi treinado. Use `.train(data)` antes de `.transform(data)`.")
        return self.pipeline_model.transform(data)

    def save_model(self, path):
        """
        Salva o modelo treinado em um caminho específico.
        """
        if not self.pipeline_model:
            raise ValueError("O modelo ainda não foi treinado. Não é possível salvar um modelo vazio.")
        
        # Certifica-se de que o diretório existe
        os.makedirs(path, exist_ok=True)
        self.pipeline_model.write().overwrite().save(path)
        print(f"Modelo salvo em: {path}")

    @staticmethod
    def load_model(path):
        """
        Carrega um modelo previamente salvo.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"O caminho especificado não existe: {path}")
        
        loaded_model = PipelineModel.load(path)
        trainer = UpliftModelTrainer("loaded")  # Placeholder para o tipo
        trainer.pipeline_model = loaded_model
        print(f"Modelo carregado de: {path}")
        return trainer


class ModelEvaluator:
    def __init__(self):
        self.evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", metricName="areaUnderROC")

    def evaluate(self, model, test_data):
        """
        Avalia o modelo em dados de teste.
        """
        predictions = model.transform(test_data)
        auc = self.evaluator.evaluate(predictions)
        return auc

def add_class_weights(data, label_col, weight_col="classWeight"):
    """
    Adiciona uma coluna de pesos baseada no desbalanceamento entre classes.
    """
    # Calcula o total de instâncias e a proporção inversa para cada classe
    class_counts = data.groupBy(label_col).count().collect()
    total_count = sum(row["count"] for row in class_counts)
    class_weights = {row[label_col]: total_count / row["count"] for row in class_counts}

    # Cria a coluna com os pesos
    data = data.withColumn(weight_col, 
                           when(col(label_col) == 0, lit(class_weights[0]))
                           .when(col(label_col) == 1, lit(class_weights[1])))
    return data

def main():
    # Configuração do Spark
    spark = SparkSession.builder.appName("UpliftModeling").getOrCreate()

    # Dados de exemplo
    data = spark.read.csv("path/to/your/data.csv", header=True, inferSchema=True)

    # Vetorização das features
    feature_columns = [col for col in data.columns if col not in ("label", "treatment")]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    data = assembler.transform(data)

    # Adiciona pesos para classes desbalanceadas
    data = add_class_weights(data, label_col="label", weight_col="classWeight")

    # Divisão entre tratados e não tratados
    treated_data = data.filter(col("treatment") == 1)
    untreated_data = data.filter(col("treatment") == 0)

    # Divisão de treino e teste
    treated_train, treated_test = treated_data.randomSplit([0.8, 0.2], seed=42)
    untreated_train, untreated_test = untreated_data.randomSplit([0.8, 0.2], seed=42)

    # Modelos a serem testados
    model_types = ["GBT", "MLP", "LogisticRegression", "LinearSVC"]
    results = []

    for model_type in model_types:
        print(f"Treinando modelo {model_type} para tratados...")

        trainer = UpliftModelTrainer(model_type)
        treated_model = trainer.train(treated_train)

        evaluator = ModelEvaluator()
        treated_auc = evaluator.evaluate(treated_model, treated_test)

        print(f"AUC no grupo tratado ({model_type}): {treated_auc}")

        print(f"Treinando modelo {model_type} para não tratados...")

        untreated_model = trainer.train(untreated_train)
        untreated_auc = evaluator.evaluate(untreated_model, untreated_test)

        results.append({
            "model": model_type,
            "treated_auc": treated_auc,
            "untreated_auc": untreated_auc
        })

    for result in results:
        print(f"Modelo: {result['model']}, AUC (tratados): {result['treated_auc']:.4f}, AUC (não tratados): {result['untreated_auc']:.4f}")

    spark.stop()

if __name__ == "__main__":
    main()


from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

class PropensityScoreModel:
    def __init__(self, features, treatment_col="treatment", propensity_col="propensity_score"):
        self.features = features
        self.treatment_col = treatment_col
        self.propensity_col = propensity_col

        # Inicializa o pipeline com assembler e modelo
        self.assembler = VectorAssembler(inputCols=self.features, outputCol="features")
        self.model = LogisticRegression(featuresCol="features", labelCol=self.treatment_col, probabilityCol=self.propensity_col)
        self.pipeline = Pipeline(stages=[self.assembler, self.model])

    def train(self, train_data):
        """
        Treina o modelo para calcular a propensity score.
        """
        self.pipeline_model = self.pipeline.fit(train_data)
        return self.pipeline_model

    def transform(self, data):
        """
        Adiciona a propensity score aos dados.
        """
        if not hasattr(self, "pipeline_model"):
            raise ValueError("O modelo ainda não foi treinado. Use `.train(data)` antes de usar `.transform(data)`.")
        return self.pipeline_model.transform(data)

# Exemplo de uso:
# Inicialize o modelo
propensity_model = PropensityScoreModel(features=["feature1", "feature2", "feature3"], treatment_col="treatment")

# Treine o modelo com dados de treinamento
propensity_model.train(train_data)

# Adicione a coluna de propensity score aos dados
data_with_propensity = propensity_model.transform(data)







class UpliftCalculator:
    def __init__(self, treated_model, untreated_model):
        self.treated_model = treated_model
        self.untreated_model = untreated_model

    def calculate_uplift(self, data):
        """
        Calcula o uplift para cada cliente:
        Uplift = P(Y=1 | T=1) - P(Y=1 | T=0)
        """
        # Previsões para tratados
        treated_predictions = self.treated_model.transform(data).select("id", "probability").withColumnRenamed("probability", "treated_prob")

        # Previsões para não tratados
        untreated_predictions = self.untreated_model.transform(data).select("id", "probability").withColumnRenamed("probability", "untreated_prob")

        # Junta os resultados e calcula o uplift
        uplift_data = treated_predictions.join(untreated_predictions, on="id") \
            .withColumn("uplift", col("treated_prob")[1] - col("untreated_prob")[1])

        return uplift_data
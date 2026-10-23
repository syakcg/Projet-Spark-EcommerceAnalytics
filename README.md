# EcommerceAnalytics – Pipeline de Traitement Distribué (Spark & Scala)

## Présentation du Projet
**EcommerceAnalytics** est un système d'analyse distribué de données e-commerce développé en **Scala** et **Apache Spark**. L'application ingère, nettoie, valide et transforme des jeux de données multi-formats (CSV, JSON, Parquet) afin de produire des métriques analytiques à forte valeur ajoutée (KPIs marchands, segmentation RFM, analyse comportementale temporelle).

---

##  Architecture & Structure du Projet

L'application suit une architecture modulaire en couches, respectant les principes du typage fort avec `Dataset[T]` et l'externalisation des configurations via **Typesafe Config**.

```text
EcommerceAnalytics/
├── build.sbt
├── project/
│   ├── build.properties
│   └── plugins.sbt
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   ├── application.conf          # Configuration externalisée (HOCON)
│   │   │   └── data/                    # Données brutes (Transactions, Users, Products, Merchants)
│   │   └── scala/
│   │       └── com/
│   │           └── ecommerce/
│   │               ├── models/
│   │               │   └── Models.scala  # Case classes & typage des données
│   │               ├── utils/
│   │               │   └── ConfigLoader.scala # Chargeur de configuration
│   │               └── analytics/
│   │                   ├── DataIngestion.scala    # Ingestion multi-format
│   │                   ├── DataValidation.scala   # Contrôle qualité & capture des rejets
│   │                   ├── TimeFeatures.scala     # UDF temporelle
│   │                   ├── DataTransformation.scala # Jointures & Fenêtrages (Window)
│   │                   ├── Analytics.scala        # KPIs & Segmentation RFM
│   │                   ├── SparkOptimizations.scala # Caching & Broadcast Joins
│   │                   └── MainApp.scala          # Orchestrateur principal
└── output/                               # Résultats exportés (Parquet / CSV)

```
## Prérequis & Stack Technique
```
Java JDK : 11 ou 17 et plus 
Scala : 2.12.18
SBT : 1.9.7
Apache Spark : 3.4.1 (Spark Core, Spark SQL)
Typesafe Config : 1.4.2
```
## Guide d'Exécution & Commandes
1. Compilation du projet
```
sbt compile
```
2. Exécution en mode local
```
sbt run
```
Sélectionnez com.ecommerce.analytics.MainApp pour lancer l'orchestrateur principal, ou les classes de test dédiées (TestMemberA, TestMemberB).

3. Génération du Fat-JAR pour déploiement sur cluster (YARN)
```
sbt assembly
```
Le fichier JAR exécutable sera généré sous target/scala-2.12/EcommerceAnalytics-assembly-1.0.jar.
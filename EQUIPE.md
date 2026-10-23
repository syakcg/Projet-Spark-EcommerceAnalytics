

### Fichier `EQUIPE.md`


#  Composition de l'Équipe & Répartition des Rôles

**Groupe N°** : Groupe 1  
**Projet** : Système d'analyse de données e-commerce distribué (Spark & Scala)

## Membre du Groupe

```                
                        || User Git         || Mails                    || Membre   || 
========================================================================||==========||
AKOWANOU Sylvestre      == syakcg           || sylvestre.auto@gmail.com || Membre A || 
AMIAN Tanoh Léandre     == tanohleandre-225 || tanohleandre@gmail.com   || Membre B ||
ADOU Koffi Jules Landry == juleslandry      || julandry1@gmail.com      || Membre C ||

```

## Attributions des Rôles et Responsabilités

### Membre A : AKOWANOU Sylvestre – Data Ingestion & Platform Engineer
* **Rôle** : Responsable de la plateforme, de l'infrastructure de build, de l'ingestion multi-format et du contrôle de qualité des données.
* **Composants livrés** :
    * Infrastructure SBT (`build.sbt`, `project/plugins.sbt`, `build.properties`).
    * Configuration externalisée (`application.conf`, `ConfigLoader.scala`).
    * Modélisation fortement typée (`Models.scala` : `Transaction`, `User`, `Product`, `Merchant`, `QualityReportRow`).
    * Module d'ingestion sécurisée multi-format (`DataIngestion.scala`).
    * Module de validation, capture des rejets et rapport de qualité (`DataValidation.scala`).
    * Documentation du projet (`README.md`, `EQUIPE.md`, `CONTRIBUTIONS.md`).


### Membre B : AMIAN Léandre – Data Transformation Engineer
* **Rôle** : Responsable du traitement analytique, de l'enrichissement des données et des calculs avancés par fenêtrage.
* **Composants livrés** :
    * UDF d'extraction des caractéristiques temporelles (`TimeFeatures.scala`).
    * Enrichissement multi-tables par jointures distribuées (`DataTransformation.scala`).
    * Fenêtrage analytique par utilisateur (rangs de transactions, totaux).
    * Calculs glissants sur 7 jours avec fenêtres temporelles (`add7DaysRollingMetrics`).

###  Membre C : ADOU Koffi Jules  – Analytics & Performance Engineer
* **Rôle** : Responsable des métriques métier, des algorithmes de segmentation, de l'optimisation des performances Spark et de l'orchestration globale.
* **Composants livrés** :
    * Calcul des KPIs marchands et financiers (`Analytics.scala`).
    * Algorithme de segmentation client RFM avec quintiles (`computeRFMSegmentation`).
    * Stratégies d'optimisation mémoire et réseau (`SparkOptimizations.scala` : caching `StorageLevel.MEMORY_AND_DISK_SER`, `broadcast join`).
    * Orchestrateur principal et point d'entrée de l'application (`MainApp.scala`).
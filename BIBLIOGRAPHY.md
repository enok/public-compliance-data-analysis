# Complete Bibliography
## Public Compliance Data Analysis - MBA Thesis

---

## 1. Data Architecture & Engineering

### Medallion Architecture
1. **Databricks. (2023).** *What is a Medallion Architecture?* Databricks Documentation.  
   Available at: https://www.databricks.com/glossary/medallion-architecture  
   - Defines the three-layer pattern: Bronze (raw), Silver (validated), Gold (aggregated)
   - Industry standard for data lake organization and data quality management

2. **Armbrust, M., et al. (2020).** *Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores.*  
   Proceedings of the VLDB Endowment, 13(12), 3411-3424.  
   DOI: 10.14778/3415478.3415560
   - Academic foundation for incremental data processing and schema evolution
   - ACID transactions in data lakes using checkpoint mechanisms

3. **Inmon, W. H. (2005).** *Building the Data Warehouse* (4th ed.). Wiley.  
   ISBN: 978-0764599446
   - Foundational concepts of data staging and progressive transformation
   - Separation of operational and analytical data concerns

4. **Kimball, R., & Ross, M. (2013).** *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling* (3rd ed.). Wiley.  
   ISBN: 978-1118530801
   - ETL best practices: extraction, transformation, and loading patterns
   - Dimensional modeling for analytical workloads

### Cloud Data Engineering
5. **AWS. (2024).** *AWS Lake Formation Best Practices.*  
   Amazon Web Services Documentation.  
   Available at: https://docs.aws.amazon.com/lake-formation/  
   - Cloud-native implementation patterns for S3-based data lakes
   - Security and governance frameworks for public sector data

6. **AWS. (2024).** *Amazon S3 Best Practices Design Patterns.*  
   Amazon Web Services Documentation.  
   Available at: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
   - Object storage optimization for large-scale data processing
   - Partitioning strategies for analytical queries

7. **Kleppmann, M. (2017).** *Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems.* O'Reilly Media.  
   ISBN: 978-1449373320
   - Data modeling for distributed systems
   - Batch and stream processing architectures

8. **Reis, J., & Housley, M. (2022).** *Fundamentals of Data Engineering.* O'Reilly Media.  
   ISBN: 978-1098108304
   - Data pipeline architecture patterns (Bronze, Silver, Gold)
   - Orchestration and workflow management
   - "Pipelines as code" reproducibility principles

---

## 2. Statistical Methods

### Regression Analysis
9. **James, G., Witten, D., Hastie, T., & Tibshirani, R. (2021).** *An Introduction to Statistical Learning with Applications in R* (2nd ed.). Springer.  
   ISBN: 978-1071614174
   - Multiple linear regression theory and applications
   - Model selection and regularization techniques
   - Cross-validation and resampling methods

10. **Montgomery, D. C., Peck, E. A., & Vining, G. G. (2012).** *Introduction to Linear Regression Analysis* (5th ed.). Wiley.  
   ISBN: 978-0470542811
   - Diagnostic checking in regression models
   - Multicollinearity detection and treatment (VIF)
   - Residual analysis and model assumptions

11. **Fox, J. (2015).** *Applied Regression Analysis and Generalized Linear Models* (3rd ed.). SAGE Publications.  
    ISBN: 978-1452205663
    - Regression diagnostics (Cook's Distance, leverage, influence)
    - Heteroscedasticity tests (Breusch-Pagan, White)
    - Transformation and weighted least squares

12. **Wooldridge, J. M. (2020).** *Introductory Econometrics: A Modern Approach* (7th ed.). Cengage Learning.  
    ISBN: 978-1337558860
    - Econometric theory and causal inference
    - Panel data methods and fixed effects
    - Heteroscedasticity-robust standard errors (HC3)
    - Instrumental variables and 2SLS

### Hypothesis Testing & ANOVA
13. **Field, A. (2017).** *Discovering Statistics Using IBM SPSS Statistics* (5th ed.). SAGE Publications.  
    ISBN: 978-1526419521
    - One-way ANOVA for group comparisons
    - Post-hoc tests (Tukey HSD)
    - Effect size interpretation

14. **Kutner, M. H., Nachtsheim, C. J., Neter, J., & Li, W. (2005).** *Applied Linear Statistical Models* (5th ed.). McGraw-Hill.  
    ISBN: 978-0073108742
    - Analysis of variance and covariance
    - Multiple comparison procedures
    - Experimental design principles

### Correlation Analysis
15. **Cohen, J., Cohen, P., West, S. G., & Aiken, L. S. (2003).** *Applied Multiple Regression/Correlation Analysis for the Behavioral Sciences* (3rd ed.). Routledge.  
    ISBN: 978-0805822236
    - Pearson correlation and significance testing
    - Partial and semi-partial correlations
    - Correlation vs. causation considerations

### Diagnostic Methods
16. **Breusch, T. S., & Pagan, A. R. (1979).** *A Simple Test for Heteroscedasticity and Random Coefficient Variation.*  
    Econometrica, 47(5), 1287-1294.  
    DOI: 10.2307/1911963
    - Breusch-Pagan test for heteroscedasticity
    - Lagrange multiplier test statistic
    - Regression diagnostic for variance homogeneity

17. **Tukey, J. W. (1977).** *Exploratory Data Analysis.* Addison-Wesley.  
    ISBN: 978-0201076165
    - Boxplot and five-number summary
    - Exploratory data visualization methods
    - Resistant line fitting and smoothing

### Causal Inference & Matching
18. **Angrist, J. D., & Pischke, J. S. (2009).** *Mostly Harmless Econometrics: An Empiricist's Companion.* Princeton University Press.  
    ISBN: 978-0691120355
    - Causal inference in observational studies
    - Regression discontinuity designs
    - Instrumental variables and natural experiments

19. **Rosenbaum, P. R., & Rubin, D. B. (1983).** *The Central Role of the Propensity Score in Observational Studies for Causal Effects.*  
    Biometrika, 70(1), 41-55.  
    DOI: 10.1093/biomet/70.1.41
    - Propensity score matching methodology
    - Balancing covariates in observational studies
    - Causal inference without randomization

20. **Stuart, E. A. (2010).** *Matching Methods for Causal Inference: A Review and a Look Forward.*  
    Statistical Science, 25(1), 1-21.  
    DOI: 10.1214/09-STS313
    - Propensity score matching best practices
    - Matching estimator selection
    - Balance assessment and sensitivity analysis

### Regularization Methods
21. **Zou, H., & Hastie, T. (2005).** *Regularization and Variable Selection via the Elastic Net.*  
    Journal of the Royal Statistical Society: Series B, 67(2), 301-320.  
    DOI: 10.1111/j.1467-9868.2005.00503.x
    - Elastic net regularization combining L1 and L2 penalties
    - Variable selection for correlated predictors
    - Grouped variable selection

---

## 3. Machine Learning & Predictive Modeling

### Supervised Learning
14. **Hastie, T., Tibshirani, R., & Friedman, J. (2009).** *The Elements of Statistical Learning: Data Mining, Inference, and Prediction* (2nd ed.). Springer.  
    ISBN: 978-0387848570
    - Classification and regression trees (CART)
    - Random forests and ensemble methods
    - Gradient boosting machines
    - Model assessment and selection

15. **Géron, A. (2022).** *Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow* (3rd ed.). O'Reilly Media.  
    ISBN: 978-1098125974
    - Practical implementation of ML algorithms in Python
    - Feature engineering and selection
    - Hyperparameter tuning with GridSearchCV
    - Model evaluation metrics

### Model Evaluation & Validation
16. **Kuhn, M., & Johnson, K. (2013).** *Applied Predictive Modeling.* Springer.  
    ISBN: 978-1461468486
    - Cross-validation techniques
    - Performance metrics (RMSE, MAE, R²)
    - Feature importance analysis
    - Model interpretability

17. **Molnar, C. (2022).** *Interpretable Machine Learning: A Guide for Making Black Box Models Explainable* (2nd ed.).  
    Available at: https://christophm.github.io/interpretable-ml-book/
    - Feature importance methods
    - Partial dependence plots
    - SHAP values for model interpretation

### Classification Methods
18. **Bishop, C. M. (2006).** *Pattern Recognition and Machine Learning.* Springer.  
    ISBN: 978-0387310732
    - Logistic regression for binary classification
    - Support vector machines
    - Probabilistic graphical models

19. **Fawcett, T. (2006).** *An Introduction to ROC Analysis.* Pattern Recognition Letters, 27(8), 861-874.  
    DOI: 10.1016/j.patrec.2005.10.010
    - ROC curve construction and interpretation
    - AUC as performance metric
    - Threshold selection strategies

### Clustering & Dimensionality Reduction
28. **MacQueen, J. (1967).** *Some Methods for Classification and Analysis of Multivariate Observations.*  
    Proceedings of the 5th Berkeley Symposium on Mathematical Statistics and Probability, 1, 281-297.
    - k-means clustering algorithm foundation
    - Multivariate data partitioning methods
    - Iterative cluster centroid optimization

29. **Hartigan, J. A., & Wong, M. A. (1979).** *Algorithm AS 136: A K-Means Clustering Algorithm.*  
    Journal of the Royal Statistical Society: Series C, 28(1), 100-108.  
    DOI: 10.2307/2346830
    - Efficient k-means implementation
    - Clustering convergence criteria
    - Applied Statistics algorithm standard

30. **Rousseeuw, P. J. (1987).** *Silhouettes: A Graphical Aid to the Interpretation and Validation of Cluster Analysis.*  
    Journal of Computational and Applied Mathematics, 20, 53-65.  
    DOI: 10.1016/0377-0427(87)90125-7
    - Silhouette coefficient for cluster validation
    - Graphical cluster quality assessment
    - Within-cluster vs. between-cluster distance metric

31. **Jolliffe, I. T. (2002).** *Principal Component Analysis* (2nd ed.). Springer.  
    ISBN: 978-0387954424
    - PCA theory and dimensionality reduction
    - Eigenvalue decomposition for feature extraction
    - Scree plot interpretation for component selection

### Ensemble Methods
32. **Breiman, L. (2001).** *Random Forests.*  
    Machine Learning, 45(1), 5-32.  
    DOI: 10.1023/A:1010933404324
    - Random Forest algorithm foundation
    - Bagging and feature randomization
    - Variable importance measures

### Software Implementation
33. **Pedregosa, F., et al. (2011).** *Scikit-learn: Machine Learning in Python.*  
    Journal of Machine Learning Research, 12, 2825-2830.
    - Python machine learning library
    - Unified API for classification and regression
    - Integration with NumPy and SciPy ecosystems

---

## 4. Public Administration & Compliance

### Transparency & Accountability
20. **Brasil. (2011).** *Lei nº 12.527, de 18 de novembro de 2011 - Lei de Acesso à Informação (LAI).*  
    Available at: http://www.planalto.gov.br/ccivil_03/_ato2011-2014/2011/lei/l12527.htm
    - Legal framework for public data transparency in Brazil
    - Citizen rights to government information
    - Obligations of public entities

21. **Brasil. (2013).** *Decreto nº 7.724, de 16 de maio de 2013 - Regulamenta a Lei de Acesso à Informação e disciplina o Cadastro de Empresas Inidôneas e Suspensas (CEIS).*  
    Available at: http://www.planalto.gov.br/ccivil_03/_ato2011-2014/2013/decreto/d7724.htm
    - Establishes the CEIS sanctions registry framework
    - Defines ineligibility criteria for sanctioned companies
    - Implementation of transparency sanctions under LAI

22. **Controladoria-Geral da União (CGU). (2024).** *Portal da Transparência do Governo Federal.*  
    Available at: https://portaldatransparencia.gov.br/
    - Federal transfer data (Transferências Fundo-a-Fundo)
    - Sanctions registries (CEIS, CNEP, CEAF, CEPIM)
    - API documentation for programmatic access

37. **Michener, G. (2015).** *Open Data and Open Government: A Review of the Literature.*  
    Revista de Administração Pública, 49(5), 1021-1038.  
    DOI: 10.1590/0034-7612145257
    - Open government data and transparency initiatives
    - Data availability for public policy analysis
    - Citizen engagement with government information

22. **Tribunal de Contas da União (TCU). (2023).** *Referencial de Combate à Fraude e Corrupção.*  
    Available at: https://portal.tcu.gov.br/
    - Fraud detection methodologies
    - Red flags for corruption risk
    - Audit standards for public spending

38. **Power, M. (2007).** *Organized Uncertainty: Designing a World of Risk Management.*  
    Oxford University Press.  
    ISBN: 978-0199296195
    - Institutional capacity for risk detection and auditing
    - Audit society theory and regulatory frameworks
    - Detection capacity as institutional signal

### Corruption Detection & Public Finance
23. **Rose-Ackerman, S., & Palifka, B. J. (2016).** *Corruption and Government: Causes, Consequences, and Reform* (2nd ed.). Cambridge University Press.  
    ISBN: 978-1107441095
    - Economic theory of corruption
    - Institutional factors affecting corruption
    - Anti-corruption strategies

24. **Ferraz, C., & Finan, F. (2008).** *Exposing Corrupt Politicians: The Effects of Brazil's Publicly Released Audits on Electoral Outcomes.*  
    The Quarterly Journal of Economics, 123(2), 703-745.  
    DOI: 10.1162/qjec.2008.123.2.703
    - Impact of transparency on political accountability in Brazil
    - Audit findings and electoral consequences
    - Empirical evidence from Brazilian municipalities

40. **Ferraz, C., & Finan, F. (2011).** *Electoral Accountability and Corruption: Evidence from the Audits of Local Governments.*  
    American Economic Journal: Applied Economics, 3(2), 33-56.  
    DOI: 10.1257/app.3.2.33
    - Electoral accountability mechanisms in municipal governments
    - Corruption detection and voter response
    - Brazilian anti-corruption audit program effects

25. **Olken, B. A., & Pande, R. (2012).** *Corruption in Developing Countries.*  
    Annual Review of Economics, 4, 479-509.  
    DOI: 10.1146/annurev-economics-080511-110917
    - Measurement challenges in corruption research
    - Experimental approaches to studying corruption
    - Policy interventions and their effectiveness

27. **Brollo, F., Nannicini, T., Perotti, R., & Tabellini, G. (2013).** *The Political Resource Curse.*  
    American Economic Review, 103(5), 1759-1796.  
    DOI: 10.1257/aer.103.5.1759
    - Federal transfers and political corruption incentives
    - "Maldição dos recursos políticos" in Brazilian federalism
    - Audit evidence from Brazilian municipalities

### Public Spending Efficiency
28. **Afonso, A., Schuknecht, L., & Tanzi, V. (2005).** *Public Sector Efficiency: An International Comparison.*  
    Public Choice, 123(3-4), 321-347.  
    DOI: 10.1007/s11127-005-7165-2
    - Methodologies for measuring public sector efficiency
    - Cross-country comparisons
    - Input-output analysis frameworks

29. **Gupta, S., & Verhoeven, M. (2001).** *The Efficiency of Government Expenditure: Experiences from Africa.*  
    Journal of Policy Modeling, 23(4), 433-467.  
    DOI: 10.1016/S0161-8938(00)00036-3
    - Data envelopment analysis (DEA) for efficiency measurement
    - Social spending effectiveness
    - Determinants of spending efficiency

30. **OECD. (2022).** *Government at a Glance 2021.*  
    OECD Publishing.  
    DOI: 10.1787/gov_glance-2021-en
    - International comparison of public spending efficiency
    - Public sector performance indicators
    - Benchmarking government effectiveness

31. **European Court of Auditors. (2020).** *Audit of EU Support for Public Financial Management in Partner Countries.*  
    ECA Special Report.  
    Available at: https://www.eca.europa.eu/
    - Public financial management audit frameworks
    - External audit capacity assessment
    - Fiscal oversight best practices

---

## 5. Data Sources & Official Statistics

### IBGE Census Data
28. **Instituto Brasileiro de Geografia e Estatística (IBGE). (2010).** *Censo Demográfico 2010.*  
    Available at: https://www.ibge.gov.br/estatisticas/sociais/populacao/9662-censo-demografico-2010.html
    - Population and housing characteristics
    - Socioeconomic indicators (income, literacy, education)
    - Municipal-level aggregations

29. **Instituto Brasileiro de Geografia e Estatística (IBGE). (2022).** *Censo Demográfico 2022.*  
    Available at: https://www.ibge.gov.br/estatisticas/sociais/populacao/22827-censo-demografico-2022.html
    - Updated demographic and socioeconomic data
    - Comparability with 2010 Census
    - Methodological notes and data quality

30. **IBGE. (2024).** *API SIDRA - Sistema IBGE de Recuperação Automática.*  
    Available at: https://apisidra.ibge.gov.br/
    - Programmatic access to IBGE statistical tables
    - API documentation and endpoints
    - Data aggregation levels (state, municipality, region)

### Federal Transfer Systems
31. **Ministério da Economia. (2024).** *Sistema de Transferências Fundo-a-Fundo.*  
    Available at: https://www.gov.br/economia/pt-br
    - Legal framework for federal transfers
    - Transfer modalities and eligibility criteria
    - Monitoring and accountability mechanisms

32. **Secretaria do Tesouro Nacional (STN). (2024).** *SICONFI - Sistema de Informações Contábeis e Fiscais do Setor Público Brasileiro.*  
    Available at: https://siconfi.tesouro.gov.br/
    - Municipal fiscal data
    - Budget execution information
    - Debt and financial indicators

47. **Banco Central do Brasil (BCB). (2024).** *Sistema de Gerenciamento de Séries Temporais (SGS) - Código 433 IPCA.*  
    Available at: https://www.bcb.gov.br/
    - IPCA historical inflation series
    - Deflation indices for monetary values
    - API access for programmatic retrieval

48. **Controladoria-Geral da União (CGU). (2023).** *Relatório Anual de Gestão - Fiscalização e Controle.*  
    Available at: https://www.gov.br/cgu/
    - Annual audit findings summary
    - Municipal sanctions data compilation
    - Anti-corruption program evaluation

---

## 6. Technical Tools & Software

### Python & Data Science Libraries
34. **McKinney, W. (2022).** *Python for Data Analysis: Data Wrangling with pandas, NumPy, and Jupyter* (3rd ed.). O'Reilly Media.  
    ISBN: 978-1098104030
    - pandas DataFrame operations
    - Data cleaning and transformation
    - Time series analysis

35. **VanderPlas, J. (2016).** *Python Data Science Handbook: Essential Tools for Working with Data.* O'Reilly Media.  
    ISBN: 978-1491912058
    - NumPy for numerical computing
    - Matplotlib and Seaborn for visualization
    - Scikit-learn for machine learning

36. **Seabold, S., & Perktold, J. (2010).** *statsmodels: Econometric and statistical modeling with Python.*  
    Proceedings of the 9th Python in Science Conference, 92-96.  
    Available at: https://www.statsmodels.org/
    - OLS regression implementation
    - Statistical tests (heteroscedasticity, normality)
    - Time series models

### Cloud Computing & Infrastructure
38. **Wittig, M., & Wittig, A. (2023).** *Amazon Web Services in Action* (3rd ed.). Manning Publications.  
    ISBN: 978-1633439160
    - AWS S3 for object storage
    - IAM security best practices
    - Infrastructure as code with CloudFormation/Terraform

39. **Brikman, Y. (2022).** *Terraform: Up & Running: Writing Infrastructure as Code* (3rd ed.). O'Reilly Media.  
    ISBN: 978-1098116743
    - Infrastructure as code principles
    - AWS resource provisioning
    - State management and collaboration

---

## 7. Research Methodology & Ethics

### Quantitative Research Methods
40. **Creswell, J. W., & Creswell, J. D. (2017).** *Research Design: Qualitative, Quantitative, and Mixed Methods Approaches* (5th ed.). SAGE Publications.  
    ISBN: 978-1506386706
    - Research design frameworks
    - Quantitative data analysis strategies
    - Validity and reliability considerations

41. **Hair, J. F., Black, W. C., Babin, B. J., & Anderson, R. E. (2018).** *Multivariate Data Analysis* (8th ed.). Cengage Learning.  
    ISBN: 978-1473756540
    - Multivariate statistical techniques
    - Factor analysis and structural equation modeling
    - Cluster analysis and discriminant analysis

### Data Ethics & Privacy
42. **Floridi, L., & Taddeo, M. (2016).** *What is Data Ethics?*  
    Philosophical Transactions of the Royal Society A, 374(2083), 20160360.  
    DOI: 10.1098/rsta.2016.0360
    - Ethical frameworks for data science
    - Privacy, transparency, and accountability
    - Responsible data use in research

43. **Brasil. (2018).** *Lei Geral de Proteção de Dados (LGPD) - Lei nº 13.709/2018.*  
    Available at: http://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm
    - Brazilian data protection regulations
    - Personal data processing principles
    - Rights of data subjects

---

## 8. Geospatial Analysis

### Spatial Analysis Theory
42. **Openshaw, S. (1984).** *The Modifiable Areal Unit Problem.* Geo Books.  
    ISBN: 978-0947714008
    - MAUP: aggregation effects on spatial analysis
    - Scale and zoning effects on statistical results
    - Critical considerations for municipal-level analysis

### Geospatial Tools
56. **QGIS Development Team. (2024).** *QGIS Geographic Information System.*  
    Open Source Geospatial Foundation.  
    Available at: https://qgis.org/
    - Open-source geospatial visualization
    - Shapefile processing and choropleth mapping
    - Export capabilities for publication-quality figures

53. **Bivand, R. S., Pebesma, E., & Gómez-Rubio, V. (2013).** *Applied Spatial Data Analysis with R* (2nd ed.). Springer.  
    ISBN: 978-1461476177
    - Spatial data structures and operations
    - Spatial autocorrelation analysis
    - Geostatistical methods

57. **Anselin, L. (1988).** *Spatial Econometrics: Methods and Models.* Springer.  
    ISBN: 978-9024737352
    - Spatial regression models
    - Spatial dependence and heterogeneity
    - Local indicators of spatial association (LISA)

---

## 9. Reproducible Research

54. **Gandrud, C. (2020).** *Reproducible Research with R and RStudio* (3rd ed.). CRC Press.  
    ISBN: 978-0367143985
    - Version control with Git
    - Literate programming principles
    - Documentation best practices

55. **Wickham, H. (2014).** *Tidy Data.*  
    Journal of Statistical Software, 59(10), 1-23.  
    DOI: 10.18637/jss.v059.i10
    - Tidy data structure and semantics
    - Data reshaping and normalization principles
    - Foundation for data cleaning workflows

59. **Wickham, H., & Grolemund, G. (2017).** *R for Data Science: Import, Tidy, Transform, Visualize, and Model Data.* O'Reilly Media.  
    ISBN: 978-1491910399
    - Tidy data principles
    - Reproducible workflows
    - Data visualization best practices

---

## 10. Domain-Specific Applications

### Public Sector Analytics
60. **Provost, F., & Fawcett, T. (2013).** *Data Science for Business: What You Need to Know about Data Mining and Data-Analytic Thinking.* O'Reilly Media.  
    ISBN: 978-1449361327
    - Business problem framing
    - Analytical thinking frameworks
    - Model deployment considerations

61. **Davenport, T. H., & Harris, J. G. (2017).** *Competing on Analytics: Updated, with a New Introduction: The New Science of Winning.* Harvard Business Review Press.  
    ISBN: 978-1633693722
    - Analytics-driven decision making
    - Organizational capabilities for analytics
    - Change management for data initiatives

---

## 11. Thesis Repository & Data

### Author's Reproducible Research
62. **Jesus, E. A. (2026).** *Public Compliance Data Analysis: Reproducible Pipeline on Medallion Architecture.*  
    GitHub Repository.  
    Available at: https://github.com/enok/public-compliance-data-analysis
    - Complete source code and documentation
    - Bronze-Silver-Gold data pipeline implementation
    - Bilingual analysis notebooks (PT/EN)
    - 137 automated tests with 87% code coverage
    - AWS S3 data lake with IBGE and Transparency Portal integration

---

## Software & Package Citations

### Python Packages Used
- **pandas:** McKinney, W. (2010). Data Structures for Statistical Computing in Python. Proceedings of the 9th Python in Science Conference, 56-61.
- **NumPy:** Harris, C. R., et al. (2020). Array programming with NumPy. Nature, 585(7825), 357-362.
- **scikit-learn:** Pedregosa, F., et al. (2011). Scikit-learn: Machine Learning in Python. Journal of Machine Learning Research, 12, 2825-2830.
- **matplotlib:** Hunter, J. D. (2007). Matplotlib: A 2D Graphics Environment. Computing in Science & Engineering, 9(3), 90-95.
- **seaborn:** Waskom, M. L. (2021). seaborn: statistical data visualization. Journal of Open Source Software, 6(60), 3021.
- **boto3:** Amazon Web Services. (2024). Boto3 Documentation. Available at: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
- **statsmodels:** Seabold, S., & Perktold, J. (2010). statsmodels: Econometric and statistical modeling with Python. Proceedings of the 9th Python in Science Conference.

---

## Notes on Citation Style

This bibliography follows a hybrid citation style appropriate for interdisciplinary research:
- **Academic papers:** Author-date format with DOI when available
- **Books:** Full bibliographic information with ISBN
- **Technical documentation:** Organization name, year, title, and URL
- **Government sources:** Official agency name and legal citation format

All sources were accessed and verified as of February 2026.

---

**Last Updated:** April 20, 2026  
**Thesis:** Public Compliance Data Analysis - MBA Data Science & Analytics, USP/Esalq  
**Author:** Enok Antônio de Jesus

---

**Note:** For the Portuguese version of this bibliography, see [`BIBLIOGRAPHY.pt-BR.md`](BIBLIOGRAPHY.pt-BR.md)

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

---

## 2. Statistical Methods

### Regression Analysis
8. **James, G., Witten, D., Hastie, T., & Tibshirani, R. (2021).** *An Introduction to Statistical Learning with Applications in R* (2nd ed.). Springer.  
   ISBN: 978-1071614174
   - Multiple linear regression theory and applications
   - Model selection and regularization techniques
   - Cross-validation and resampling methods

9. **Montgomery, D. C., Peck, E. A., & Vining, G. G. (2012).** *Introduction to Linear Regression Analysis* (5th ed.). Wiley.  
   ISBN: 978-0470542811
   - Diagnostic checking in regression models
   - Multicollinearity detection and treatment (VIF)
   - Residual analysis and model assumptions

10. **Fox, J. (2015).** *Applied Regression Analysis and Generalized Linear Models* (3rd ed.). SAGE Publications.  
    ISBN: 978-1452205663
    - Regression diagnostics (Cook's Distance, leverage, influence)
    - Heteroscedasticity tests (Breusch-Pagan, White)
    - Transformation and weighted least squares

### Hypothesis Testing & ANOVA
11. **Field, A. (2017).** *Discovering Statistics Using IBM SPSS Statistics* (5th ed.). SAGE Publications.  
    ISBN: 978-1526419521
    - One-way ANOVA for group comparisons
    - Post-hoc tests (Tukey HSD)
    - Effect size interpretation

12. **Kutner, M. H., Nachtsheim, C. J., Neter, J., & Li, W. (2005).** *Applied Linear Statistical Models* (5th ed.). McGraw-Hill.  
    ISBN: 978-0073108742
    - Analysis of variance and covariance
    - Multiple comparison procedures
    - Experimental design principles

### Correlation Analysis
13. **Cohen, J., Cohen, P., West, S. G., & Aiken, L. S. (2003).** *Applied Multiple Regression/Correlation Analysis for the Behavioral Sciences* (3rd ed.). Routledge.  
    ISBN: 978-0805822236
    - Pearson correlation and significance testing
    - Partial and semi-partial correlations
    - Correlation vs. causation considerations

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

---

## 4. Public Administration & Compliance

### Transparency & Accountability
20. **Brasil. (2011).** *Lei nº 12.527, de 18 de novembro de 2011 - Lei de Acesso à Informação (LAI).*  
    Available at: http://www.planalto.gov.br/ccivil_03/_ato2011-2014/2011/lei/l12527.htm
    - Legal framework for public data transparency in Brazil
    - Citizen rights to government information
    - Obligations of public entities

21. **Controladoria-Geral da União (CGU). (2024).** *Portal da Transparência do Governo Federal.*  
    Available at: https://portaldatransparencia.gov.br/
    - Federal transfer data (Transferências Fundo-a-Fundo)
    - Sanctions registries (CEIS, CNEP, CEAF, CEPIM)
    - API documentation for programmatic access

22. **Tribunal de Contas da União (TCU). (2023).** *Referencial de Combate à Fraude e Corrupção.*  
    Available at: https://portal.tcu.gov.br/
    - Fraud detection methodologies
    - Red flags for corruption risk
    - Audit standards for public spending

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

25. **Olken, B. A., & Pande, R. (2012).** *Corruption in Developing Countries.*  
    Annual Review of Economics, 4, 479-509.  
    DOI: 10.1146/annurev-economics-080511-110917
    - Measurement challenges in corruption research
    - Experimental approaches to studying corruption
    - Policy interventions and their effectiveness

### Public Spending Efficiency
26. **Afonso, A., Schuknecht, L., & Tanzi, V. (2005).** *Public Sector Efficiency: An International Comparison.*  
    Public Choice, 123(3-4), 321-347.  
    DOI: 10.1007/s11127-005-7165-2
    - Methodologies for measuring public sector efficiency
    - Cross-country comparisons
    - Input-output analysis frameworks

27. **Gupta, S., & Verhoeven, M. (2001).** *The Efficiency of Government Expenditure: Experiences from Africa.*  
    Journal of Policy Modeling, 23(4), 433-467.  
    DOI: 10.1016/S0161-8938(00)00036-3
    - Data envelopment analysis (DEA) for efficiency measurement
    - Social spending effectiveness
    - Determinants of spending efficiency

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

---

## 6. Technical Tools & Software

### Python & Data Science Libraries
33. **McKinney, W. (2022).** *Python for Data Analysis: Data Wrangling with pandas, NumPy, and Jupyter* (3rd ed.). O'Reilly Media.  
    ISBN: 978-1098104030
    - pandas DataFrame operations
    - Data cleaning and transformation
    - Time series analysis

34. **VanderPlas, J. (2016).** *Python Data Science Handbook: Essential Tools for Working with Data.* O'Reilly Media.  
    ISBN: 978-1491912058
    - NumPy for numerical computing
    - Matplotlib and Seaborn for visualization
    - Scikit-learn for machine learning

35. **Seabold, S., & Perktold, J. (2010).** *statsmodels: Econometric and statistical modeling with Python.*  
    Proceedings of the 9th Python in Science Conference, 92-96.  
    Available at: https://www.statsmodels.org/
    - OLS regression implementation
    - Statistical tests (heteroscedasticity, normality)
    - Time series models

### Cloud Computing & Infrastructure
36. **Wittig, M., & Wittig, A. (2023).** *Amazon Web Services in Action* (3rd ed.). Manning Publications.  
    ISBN: 978-1633439160
    - AWS S3 for object storage
    - IAM security best practices
    - Infrastructure as code with CloudFormation/Terraform

37. **Brikman, Y. (2022).** *Terraform: Up & Running: Writing Infrastructure as Code* (3rd ed.). O'Reilly Media.  
    ISBN: 978-1098116743
    - Infrastructure as code principles
    - AWS resource provisioning
    - State management and collaboration

### Workflow Orchestration
38. **Apache Airflow Documentation. (2024).** *Apache Airflow.*  
    Available at: https://airflow.apache.org/
    - DAG (Directed Acyclic Graph) design patterns
    - Task scheduling and dependencies
    - AWS MWAA (Managed Workflows for Apache Airflow)

---

## 7. Research Methodology & Ethics

### Quantitative Research Methods
39. **Creswell, J. W., & Creswell, J. D. (2017).** *Research Design: Qualitative, Quantitative, and Mixed Methods Approaches* (5th ed.). SAGE Publications.  
    ISBN: 978-1506386706
    - Research design frameworks
    - Quantitative data analysis strategies
    - Validity and reliability considerations

40. **Hair, J. F., Black, W. C., Babin, B. J., & Anderson, R. E. (2018).** *Multivariate Data Analysis* (8th ed.). Cengage Learning.  
    ISBN: 978-1473756540
    - Multivariate statistical techniques
    - Factor analysis and structural equation modeling
    - Cluster analysis and discriminant analysis

### Data Ethics & Privacy
41. **Floridi, L., & Taddeo, M. (2016).** *What is Data Ethics?*  
    Philosophical Transactions of the Royal Society A, 374(2083), 20160360.  
    DOI: 10.1098/rsta.2016.0360
    - Ethical frameworks for data science
    - Privacy, transparency, and accountability
    - Responsible data use in research

42. **Brasil. (2018).** *Lei Geral de Proteção de Dados (LGPD) - Lei nº 13.709/2018.*  
    Available at: http://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm
    - Brazilian data protection regulations
    - Personal data processing principles
    - Rights of data subjects

---

## 8. Geospatial Analysis

43. **Bivand, R. S., Pebesma, E., & Gómez-Rubio, V. (2013).** *Applied Spatial Data Analysis with R* (2nd ed.). Springer.  
    ISBN: 978-1461476177
    - Spatial data structures and operations
    - Spatial autocorrelation analysis
    - Geostatistical methods

44. **Anselin, L. (1988).** *Spatial Econometrics: Methods and Models.* Springer.  
    ISBN: 978-9024737352
    - Spatial regression models
    - Spatial dependence and heterogeneity
    - Local indicators of spatial association (LISA)

---

## 9. Reproducible Research

45. **Gandrud, C. (2020).** *Reproducible Research with R and RStudio* (3rd ed.). CRC Press.  
    ISBN: 978-0367143985
    - Version control with Git
    - Literate programming principles
    - Documentation best practices

46. **Wickham, H., & Grolemund, G. (2017).** *R for Data Science: Import, Tidy, Transform, Visualize, and Model Data.* O'Reilly Media.  
    ISBN: 978-1491910399
    - Tidy data principles
    - Reproducible workflows
    - Data visualization best practices

---

## 10. Domain-Specific Applications

### Public Sector Analytics
47. **Provost, F., & Fawcett, T. (2013).** *Data Science for Business: What You Need to Know about Data Mining and Data-Analytic Thinking.* O'Reilly Media.  
    ISBN: 978-1449361327
    - Business problem framing
    - Analytical thinking frameworks
    - Model deployment considerations

48. **Davenport, T. H., & Harris, J. G. (2017).** *Competing on Analytics: Updated, with a New Introduction: The New Science of Winning.* Harvard Business Review Press.  
    ISBN: 978-1633693722
    - Analytics-driven decision making
    - Organizational capabilities for analytics
    - Change management for data initiatives

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

**Last Updated:** February 9, 2026  
**Thesis:** Public Compliance Data Analysis - MBA Data Science & Analytics, USP/Esalq  
**Author:** Enok Antônio de Jesus

---

**Note:** For the Portuguese version of this bibliography, see [`BIBLIOGRAPHY.pt-BR.md`](BIBLIOGRAPHY.pt-BR.md)

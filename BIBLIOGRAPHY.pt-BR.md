# Bibliografia Completa
## Análise de Dados de Compliance Pública - Tese de MBA

---

## 1. Arquitetura de Dados & Engenharia

### Arquitetura Medallion
1. **Databricks. (2023).** *What is a Medallion Architecture?* Databricks Documentation.  
   Disponível em: https://www.databricks.com/glossary/medallion-architecture  
   - Define o padrão de três camadas: Bronze (bruto), Silver (validado), Gold (agregado)
   - Padrão da indústria para organização de data lake e gestão de qualidade de dados

2. **Armbrust, M., et al. (2020).** *Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores.*  
   Proceedings of the VLDB Endowment, 13(12), 3411-3424.  
   DOI: 10.14778/3415478.3415560
   - Fundação acadêmica para processamento incremental de dados e evolução de schema
   - Transações ACID em data lakes usando mecanismos de checkpoint

3. **Inmon, W. H. (2005).** *Building the Data Warehouse* (4th ed.). Wiley.  
   ISBN: 978-0764599446
   - Conceitos fundamentais de staging de dados e transformação progressiva
   - Separação de preocupações de dados operacionais e analíticos

4. **Kimball, R., & Ross, M. (2013).** *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling* (3rd ed.). Wiley.  
   ISBN: 978-1118530801
   - Melhores práticas de ETL: padrões de extração, transformação e carga
   - Modelagem dimensional para cargas de trabalho analíticas

### Engenharia de Dados em Nuvem
5. **AWS. (2024).** *AWS Lake Formation Best Practices.*  
   Amazon Web Services Documentation.  
   Disponível em: https://docs.aws.amazon.com/lake-formation/  
   - Padrões de implementação cloud-native para data lakes baseados em S3
   - Frameworks de segurança e governança para dados do setor público

6. **AWS. (2024).** *Amazon S3 Best Practices Design Patterns.*  
   Amazon Web Services Documentation.  
   Disponível em: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
   - Otimização de armazenamento de objetos para processamento de dados em larga escala
   - Estratégias de particionamento para consultas analíticas

7. **Kleppmann, M. (2017).** *Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems.* O'Reilly Media.  
   ISBN: 978-1449373320
   - Modelagem de dados para sistemas distribuídos
   - Arquiteturas de processamento em lote e streaming

---

## 2. Métodos Estatísticos

### Análise de Regressão
8. **James, G., Witten, D., Hastie, T., & Tibshirani, R. (2021).** *An Introduction to Statistical Learning with Applications in R* (2nd ed.). Springer.  
   ISBN: 978-1071614174
   - Teoria e aplicações de regressão linear múltipla
   - Técnicas de seleção e regularização de modelos
   - Validação cruzada e métodos de reamostragem

9. **Montgomery, D. C., Peck, E. A., & Vining, G. G. (2012).** *Introduction to Linear Regression Analysis* (5th ed.). Wiley.  
   ISBN: 978-0470542811
   - Verificação diagnóstica em modelos de regressão
   - Detecção e tratamento de multicolinearidade (VIF)
   - Análise de resíduos e suposições do modelo

10. **Fox, J. (2015).** *Applied Regression Analysis and Generalized Linear Models* (3rd ed.). SAGE Publications.  
    ISBN: 978-1452205663
    - Diagnósticos de regressão (Distância de Cook, alavancagem, influência)
    - Testes de heterocedasticidade (Breusch-Pagan, White)
    - Transformação e mínimos quadrados ponderados

### Testes de Hipótese & ANOVA
11. **Field, A. (2017).** *Discovering Statistics Using IBM SPSS Statistics* (5th ed.). SAGE Publications.  
    ISBN: 978-1526419521
    - ANOVA one-way para comparações de grupos
    - Testes post-hoc (Tukey HSD)
    - Interpretação de tamanho de efeito

12. **Kutner, M. H., Nachtsheim, C. J., Neter, J., & Li, W. (2005).** *Applied Linear Statistical Models* (5th ed.). McGraw-Hill.  
    ISBN: 978-0073108742
    - Análise de variância e covariância
    - Procedimentos de comparação múltipla
    - Princípios de design experimental

### Análise de Correlação
13. **Cohen, J., Cohen, P., West, S. G., & Aiken, L. S. (2003).** *Applied Multiple Regression/Correlation Analysis for the Behavioral Sciences* (3rd ed.). Routledge.  
    ISBN: 978-0805822236
    - Correlação de Pearson e testes de significância
    - Correlações parciais e semi-parciais
    - Considerações sobre correlação vs. causalidade

---

## 3. Machine Learning & Modelagem Preditiva

### Aprendizado Supervisionado
14. **Hastie, T., Tibshirani, R., & Friedman, J. (2009).** *The Elements of Statistical Learning: Data Mining, Inference, and Prediction* (2nd ed.). Springer.  
    ISBN: 978-0387848570
    - Árvores de classificação e regressão (CART)
    - Random forests e métodos de ensemble
    - Gradient boosting machines
    - Avaliação e seleção de modelos

15. **Géron, A. (2022).** *Hands-On Machine Learning with Scikit-Learn, Keras, and TensorFlow* (3rd ed.). O'Reilly Media.  
    ISBN: 978-1098125974
    - Implementação prática de algoritmos de ML em Python
    - Engenharia e seleção de features
    - Ajuste de hiperparâmetros com GridSearchCV
    - Métricas de avaliação de modelos

### Avaliação & Validação de Modelos
16. **Kuhn, M., & Johnson, K. (2013).** *Applied Predictive Modeling.* Springer.  
    ISBN: 978-1461468486
    - Técnicas de validação cruzada
    - Métricas de desempenho (RMSE, MAE, R²)
    - Análise de importância de features
    - Interpretabilidade de modelos

17. **Molnar, C. (2022).** *Interpretable Machine Learning: A Guide for Making Black Box Models Explainable* (2nd ed.).  
    Disponível em: https://christophm.github.io/interpretable-ml-book/
    - Métodos de importância de features
    - Gráficos de dependência parcial
    - Valores SHAP para interpretação de modelos

### Métodos de Classificação
18. **Bishop, C. M. (2006).** *Pattern Recognition and Machine Learning.* Springer.  
    ISBN: 978-0387310732
    - Regressão logística para classificação binária
    - Máquinas de vetores de suporte
    - Modelos gráficos probabilísticos

19. **Fawcett, T. (2006).** *An Introduction to ROC Analysis.* Pattern Recognition Letters, 27(8), 861-874.  
    DOI: 10.1016/j.patrec.2005.10.010
    - Construção e interpretação de curvas ROC
    - AUC como métrica de desempenho
    - Estratégias de seleção de limiar

---

## 4. Administração Pública & Compliance

### Transparência & Accountability
20. **Brasil. (2011).** *Lei nº 12.527, de 18 de novembro de 2011 - Lei de Acesso à Informação (LAI).*  
    Disponível em: http://www.planalto.gov.br/ccivil_03/_ato2011-2014/2011/lei/l12527.htm
    - Marco legal para transparência de dados públicos no Brasil
    - Direitos dos cidadãos à informação governamental
    - Obrigações das entidades públicas

21. **Controladoria-Geral da União (CGU). (2024).** *Portal da Transparência do Governo Federal.*  
    Disponível em: https://portaldatransparencia.gov.br/
    - Dados de transferências federais (Transferências Fundo-a-Fundo)
    - Registros de sanções (CEIS, CNEP, CEAF, CEPIM)
    - Documentação de API para acesso programático

22. **Tribunal de Contas da União (TCU). (2023).** *Referencial de Combate à Fraude e Corrupção.*  
    Disponível em: https://portal.tcu.gov.br/
    - Metodologias de detecção de fraude
    - Indicadores de risco de corrupção
    - Padrões de auditoria para gastos públicos

### Detecção de Corrupção & Finanças Públicas
23. **Rose-Ackerman, S., & Palifka, B. J. (2016).** *Corruption and Government: Causes, Consequences, and Reform* (2nd ed.). Cambridge University Press.  
    ISBN: 978-1107441095
    - Teoria econômica da corrupção
    - Fatores institucionais que afetam a corrupção
    - Estratégias anticorrupção

24. **Ferraz, C., & Finan, F. (2008).** *Exposing Corrupt Politicians: The Effects of Brazil's Publicly Released Audits on Electoral Outcomes.*  
    The Quarterly Journal of Economics, 123(2), 703-745.  
    DOI: 10.1162/qjec.2008.123.2.703
    - Impacto da transparência na accountability política no Brasil
    - Resultados de auditorias e consequências eleitorais
    - Evidências empíricas de municípios brasileiros

25. **Olken, B. A., & Pande, R. (2012).** *Corruption in Developing Countries.*  
    Annual Review of Economics, 4, 479-509.  
    DOI: 10.1146/annurev-economics-080511-110917
    - Desafios de mensuração em pesquisa sobre corrupção
    - Abordagens experimentais para estudar corrupção
    - Intervenções políticas e sua efetividade

### Eficiência de Gastos Públicos
26. **Afonso, A., Schuknecht, L., & Tanzi, V. (2005).** *Public Sector Efficiency: An International Comparison.*  
    Public Choice, 123(3-4), 321-347.  
    DOI: 10.1007/s11127-005-7165-2
    - Metodologias para medir eficiência do setor público
    - Comparações entre países
    - Frameworks de análise input-output

27. **Gupta, S., & Verhoeven, M. (2001).** *The Efficiency of Government Expenditure: Experiences from Africa.*  
    Journal of Policy Modeling, 23(4), 433-467.  
    DOI: 10.1016/S0161-8938(00)00036-3
    - Análise envoltória de dados (DEA) para medição de eficiência
    - Efetividade de gastos sociais
    - Determinantes de eficiência de gastos

---

## 5. Fontes de Dados & Estatísticas Oficiais

### Dados Censitários do IBGE
28. **Instituto Brasileiro de Geografia e Estatística (IBGE). (2010).** *Censo Demográfico 2010.*  
    Disponível em: https://www.ibge.gov.br/estatisticas/sociais/populacao/9662-censo-demografico-2010.html
    - Características populacionais e habitacionais
    - Indicadores socioeconômicos (renda, alfabetização, educação)
    - Agregações em nível municipal

29. **Instituto Brasileiro de Geografia e Estatística (IBGE). (2022).** *Censo Demográfico 2022.*  
    Disponível em: https://www.ibge.gov.br/estatisticas/sociais/populacao/22827-censo-demografico-2022.html
    - Dados demográficos e socioeconômicos atualizados
    - Comparabilidade com Censo 2010
    - Notas metodológicas e qualidade dos dados

30. **IBGE. (2024).** *API SIDRA - Sistema IBGE de Recuperação Automática.*  
    Disponível em: https://apisidra.ibge.gov.br/
    - Acesso programático a tabelas estatísticas do IBGE
    - Documentação de API e endpoints
    - Níveis de agregação de dados (estado, município, região)

### Sistemas de Transferências Federais
31. **Ministério da Economia. (2024).** *Sistema de Transferências Fundo-a-Fundo.*  
    Disponível em: https://www.gov.br/economia/pt-br
    - Marco legal para transferências federais
    - Modalidades de transferência e critérios de elegibilidade
    - Mecanismos de monitoramento e accountability

32. **Secretaria do Tesouro Nacional (STN). (2024).** *SICONFI - Sistema de Informações Contábeis e Fiscais do Setor Público Brasileiro.*  
    Disponível em: https://siconfi.tesouro.gov.br/
    - Dados fiscais municipais
    - Informações de execução orçamentária
    - Indicadores de dívida e financeiros

---

## 6. Ferramentas Técnicas & Software

### Python & Bibliotecas de Data Science
33. **McKinney, W. (2022).** *Python for Data Analysis: Data Wrangling with pandas, NumPy, and Jupyter* (3rd ed.). O'Reilly Media.  
    ISBN: 978-1098104030
    - Operações com DataFrame do pandas
    - Limpeza e transformação de dados
    - Análise de séries temporais

34. **VanderPlas, J. (2016).** *Python Data Science Handbook: Essential Tools for Working with Data.* O'Reilly Media.  
    ISBN: 978-1491912058
    - NumPy para computação numérica
    - Matplotlib e Seaborn para visualização
    - Scikit-learn para machine learning

35. **Seabold, S., & Perktold, J. (2010).** *statsmodels: Econometric and statistical modeling with Python.*  
    Proceedings of the 9th Python in Science Conference, 92-96.  
    Disponível em: https://www.statsmodels.org/
    - Implementação de regressão OLS
    - Testes estatísticos (heterocedasticidade, normalidade)
    - Modelos de séries temporais

### Computação em Nuvem & Infraestrutura
36. **Wittig, M., & Wittig, A. (2023).** *Amazon Web Services in Action* (3rd ed.). Manning Publications.  
    ISBN: 978-1633439160
    - AWS S3 para armazenamento de objetos
    - Melhores práticas de segurança IAM
    - Infraestrutura como código com CloudFormation/Terraform

37. **Brikman, Y. (2022).** *Terraform: Up & Running: Writing Infrastructure as Code* (3rd ed.). O'Reilly Media.  
    ISBN: 978-1098116743
    - Princípios de infraestrutura como código
    - Provisionamento de recursos AWS
    - Gerenciamento de estado e colaboração

---

## 7. Metodologia de Pesquisa & Ética

### Métodos de Pesquisa Quantitativa
38. **Creswell, J. W., & Creswell, J. D. (2017).** *Research Design: Qualitative, Quantitative, and Mixed Methods Approaches* (5th ed.). SAGE Publications.  
    ISBN: 978-1506386706
    - Frameworks de design de pesquisa
    - Estratégias de análise de dados quantitativos
    - Considerações de validade e confiabilidade

39. **Hair, J. F., Black, W. C., Babin, B. J., & Anderson, R. E. (2018).** *Multivariate Data Analysis* (8th ed.). Cengage Learning.  
    ISBN: 978-1473756540
    - Técnicas estatísticas multivariadas
    - Análise fatorial e modelagem de equações estruturais
    - Análise de cluster e análise discriminante

### Ética de Dados & Privacidade
40. **Floridi, L., & Taddeo, M. (2016).** *What is Data Ethics?*  
    Philosophical Transactions of the Royal Society A, 374(2083), 20160360.  
    DOI: 10.1098/rsta.2016.0360
    - Frameworks éticos para ciência de dados
    - Privacidade, transparência e accountability
    - Uso responsável de dados em pesquisa

41. **Brasil. (2018).** *Lei Geral de Proteção de Dados (LGPD) - Lei nº 13.709/2018.*  
    Disponível em: http://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm
    - Regulamentações brasileiras de proteção de dados
    - Princípios de processamento de dados pessoais
    - Direitos dos titulares de dados

---

## 8. Análise Geoespacial

42. **Bivand, R. S., Pebesma, E., & Gómez-Rubio, V. (2013).** *Applied Spatial Data Analysis with R* (2nd ed.). Springer.  
    ISBN: 978-1461476177
    - Estruturas e operações de dados espaciais
    - Análise de autocorrelação espacial
    - Métodos geoestatísticos

43. **Anselin, L. (1988).** *Spatial Econometrics: Methods and Models.* Springer.  
    ISBN: 978-9024737352
    - Modelos de regressão espacial
    - Dependência e heterogeneidade espacial
    - Indicadores locais de associação espacial (LISA)

---

## 9. Pesquisa Reproduzível

44. **Gandrud, C. (2020).** *Reproducible Research with R and RStudio* (3rd ed.). CRC Press.  
    ISBN: 978-0367143985
    - Controle de versão com Git
    - Princípios de programação literária
    - Melhores práticas de documentação

45. **Wickham, H., & Grolemund, G. (2017).** *R for Data Science: Import, Tidy, Transform, Visualize, and Model Data.* O'Reilly Media.  
    ISBN: 978-1491910399
    - Princípios de dados organizados (tidy data)
    - Workflows reproduzíveis
    - Melhores práticas de visualização de dados

---

## 10. Aplicações Específicas do Domínio

### Analytics do Setor Público
46. **Provost, F., & Fawcett, T. (2013).** *Data Science for Business: What You Need to Know about Data Mining and Data-Analytic Thinking.* O'Reilly Media.  
    ISBN: 978-1449361327
    - Enquadramento de problemas de negócio
    - Frameworks de pensamento analítico
    - Considerações de implantação de modelos

47. **Davenport, T. H., & Harris, J. G. (2017).** *Competing on Analytics: Updated, with a New Introduction: The New Science of Winning.* Harvard Business Review Press.  
    ISBN: 978-1633693722
    - Tomada de decisão orientada por analytics
    - Capacidades organizacionais para analytics
    - Gestão de mudança para iniciativas de dados

---

## Citações de Software & Pacotes

### Pacotes Python Utilizados
- **pandas:** McKinney, W. (2010). Data Structures for Statistical Computing in Python. Proceedings of the 9th Python in Science Conference, 56-61.
- **NumPy:** Harris, C. R., et al. (2020). Array programming with NumPy. Nature, 585(7825), 357-362.
- **scikit-learn:** Pedregosa, F., et al. (2011). Scikit-learn: Machine Learning in Python. Journal of Machine Learning Research, 12, 2825-2830.
- **matplotlib:** Hunter, J. D. (2007). Matplotlib: A 2D Graphics Environment. Computing in Science & Engineering, 9(3), 90-95.
- **seaborn:** Waskom, M. L. (2021). seaborn: statistical data visualization. Journal of Open Source Software, 6(60), 3021.
- **boto3:** Amazon Web Services. (2024). Boto3 Documentation. Disponível em: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
- **statsmodels:** Seabold, S., & Perktold, J. (2010). statsmodels: Econometric and statistical modeling with Python. Proceedings of the 9th Python in Science Conference.

---

## Notas sobre Estilo de Citação

Esta bibliografia segue um estilo de citação híbrido apropriado para pesquisa interdisciplinar:
- **Artigos acadêmicos:** Formato autor-data com DOI quando disponível
- **Livros:** Informação bibliográfica completa com ISBN
- **Documentação técnica:** Nome da organização, ano, título e URL
- **Fontes governamentais:** Nome da agência oficial e formato de citação legal

Todas as fontes foram acessadas e verificadas em fevereiro de 2026.

---

**Última Atualização:** 9 de fevereiro de 2026  
**Tese:** Análise de Dados de Compliance Pública - MBA Data Science & Analytics, USP/Esalq  
**Autor:** Enok Antônio de Jesus

---

**Nota:** Para a versão em inglês desta bibliografia, consulte [`BIBLIOGRAPHY.md`](BIBLIOGRAPHY.md)

# How to Use This Software

```
# 1. Clone the repository to your local hard drive

# 2. Run the following commands from within the project folder
python3 -m venv venv
pip install -r requirements.txt

# 3. to download the database (might take 4-6 hours)
python 5000_script_runner_contiguous.py

# 4. to run the test
python 0918_top_k_disease_targeted_similarity_sorted_by_evidence.py

# 5. to run the server
python 3014_server_full_scoring.py
```



---

# Affordable
Affordable is a DeSci and AI-agent-powered wiki for discovery and clinical validation of affordable medicine

Litepaper v1.1

Affordable: Revolutionizing Healthcare Through Decentralized Science (DeSci)

Abstract

Affordable is a groundbreaking initiative leveraging Decentralized Science (DeSci) to reimagine global healthcare. Our mission is to make high-impact, low-cost therapies accessible by incentivizing their development and validation through a transparent and collaborative ecosystem. 
Affordable is aimed at addressing a critical market failure in healthcare: the lack of financial incentives to clinically validate affordable therapies. Many low-cost alternatives, such as repurposed generic drugs or biosimilars, dietary supplements, diets, behavioral interventions or optimized dosing regimens (Affordable Therapies), have the potential to replace expensive standard-of-care treatments or address unmet medical needs. However, due to the absence of exclusive patents and the high cost of clinical trials, these therapies remain underutilized and unvalidated.
Affordable bridges this gap by leveraging innovative financial models such as Interventional Pharmacoeconomics (IVPE) and Pay-for-Success (PFS) contracts. These mechanisms de-risk the validation process for health insurers (private and government), enabling clinical trials to be funded without financial risk. Using IVPE, trials are funded directly from the cost-savings realized by validating affordable therapies. Through PFS, payments are contingent on achieving predefined health outcomes, ensuring that resources are allocated effectively.
By combining blockchain technology, decentralized governance, and tokenized rewards, Affordable offers a transparent, collaborative, and outcome-driven framework for funding clinical trials. Partnerships with organizations such as CrowdFundedCures.org, a registered DeSci charity, PublicGoodPharma.com, a biotech social enterprise, and blockchain platforms like Cureledger (Whitepaper), further strengthen this ecosystem, enabling the secure storage of clinical trial data and automated milestone-based payments.
Through this approach, Affordable democratizes healthcare innovation, incentivizes the validation of cost-saving therapies, and empowers communities to make public good medicines a reality. This system aligns financial incentives with improved health outcomes, unlocking new funding streams for clinical trials while addressing unmet medical needs globally.
 
The Challenge: Rising Healthcare Costs
Global healthcare expenditures are rising at unsustainable rates, driven by the increasing reliance on expensive, proprietary therapies. Many of these high-cost treatments are effective, but equally promising, affordable alternatives often go unvalidated due to a lack of financial incentives. Affordable addresses this systemic issue by creating a sustainable, incentive-driven model to:
1.	Identify cost-effective therapies.
2.	Validate these therapies through rigorous clinical trials.
3.	Reward contributors for their role in generating evidence.
4.	Decentralize access to healthcare innovations.
 
The Affordable Opportunity
Our mission is to support public good medicines that prioritize accessibility and cost savings over profit.
The Affordable Algorithm calculates cost-saving potential for therapies that are candidates for IVPE and PFS funding by combining three key factors:

1.	Global Patient Population: The estimated number of patients eligible for treatment worldwide.
2.	Cost Difference: The annual cost savings achieved by replacing the current standard of care with a low-cost therapy.
3.	Evidence Probability: A score derived from a hierarchy of evidence, which quantifies the strength of clinical support for the therapy.
   
Hierarchy of Evidence
The hierarchy assigns a weighted score to evidence supporting the likelihood of a therapy’s efficacy and safety for patients, ensuring that stronger clinical validation results in higher rankings.

Study Type	Evidence Score
Animal Studies	0.1
Case Study Series	0.2
Retrospective Studies	0.3
RCT Emulation	0.4
Prospective Studies (Single-Arm)	0.5
Phase 2 RCTs	0.6
Phase 3 RCTs	0.7
Pre-Approval Systematic Reviews	0.8
Clinical Guidelines / FDA Approval	0.9
Post-Approval Systematic Reviews	1.0
Bonus for Superiority Evidence (+10%)	+0.1
How It Works:
For example, a low-cost therapy supported by a Phase 2 RCT (0.6) with a demonstrated 10% improvement in outcomes (adding 0.1) would receive an evidence probability score of 0.7. This score is then combined with the global patient population and cost difference to determine the total cost-saving potential and assign an Affordable Algorithm Ranking 
By prioritizing therapies with robust evidence and scalable impact, the Affordable Algorithm ensures the most promising treatments are identified for validation and funding.
Interventional Pharmacoeconomics (IVPE) Funding Validation of Affordable Therapies 
What is IVPE?
Interventional Pharmacoeconomics (IVPE) is an innovative approach to healthcare cost optimization that funds clinical trials based on the cost-savings generated by validating lower-cost, alternative therapies to expensive Standard of Care (SoC). This model:
1.	Targets High-Cost Therapies: Focuses on identifying alternative therapies that can achieve similar or superior outcomes at significantly lower costs.
2.	Funding Mechanism: Uses cost-savings from health insurers to self-fund trials for dose optimization, therapy substitution, or off-label uses of generic drugs. Our partner, Public Good Pharma will engage with CROs and health insurers to fund trials as a cost-containment solution. 
3.	Blockchain Integration: Ensures transparency and accountability in IVPE funding through on-chain recording of clinical trial data via Cureledger and cost-savings analytics.
Affordable Table for IVPE Validation
The Affordable Algorithm ranks therapies eligible for IVPE funding validation based on the following criteria:
•	Cost-Savings Potential: The difference between the current standard of care and the proposed alternative.
•	Evidence Probability: Weighted scores based on the hierarchy of evidence (e.g., RCT emulation, Phase 2/3 trials, systematic reviews).
•	Global Patient Population: The number of patients eligible for treatment globally.
Medical Disclaimer:
The Affordable IVPE and Affordable PFS tables below is an example only and will be reviewed and validated by clinical advisors. This is not a statement that the treatments listed are proven to be effective for the conditions mentioned. Links to the relevant research data will be provided where applicable. These treatments should only be used under the oversight of a registered medical professional. Affordable does not make medical claims and encourages consulting healthcare professionals before considering any therapy.
 
Affordable IVPE Table  

This table highlights therapies with potential for significant healthcare cost reductions through Interventional Pharmacoeconomics (IVPE). By identifying cost-effective alternatives to high-cost treatments, the table ranks candidates suitable for IVPE funding by health insurers based on their ability to deliver savings while maintaining or improving clinical outcomes. Therapies are evaluated using evidence probability, global patient population, and cost difference, with rankings driven by their potential to optimize healthcare spending.
Affordable Algorithm Ranking	Therapy	Disease	Global Patient Population	Cost Difference ($)	Evidence Probability (Score: Evidence Type)	Annual Total Cost Reduction Potential ($)	PubMed Link 
0.70	Pembrolizumab (Dose Reduction)	Lung Cancer	1,000,000	$90,000 (90% dose reduction; $100,000 baseline)	0.6: Prospective Studies	$90 billion	Pembrolizumab Optimization

0.70	Rituximab	Multiple Sclerosis	2,800,000	$72,000 (Ocrelizumab: $80,000)	0.7: Phase 3 RCTs	$201.6 billion	Rituximab for MS

0.70	Ketamine	Treatment-Resistant Depression	20,000,000	$20,000 (Esketamine: $25,000)	0.7: Phase 3 RCTs	$400 billion	Ketamine in TRD

0.50	Ketogenic Diet	Glioblastoma	1,500,000	$90,000 ($100,000 baseline)	0.5: Prospective Studies	$135 billion	Ketogenic Diet for Cancer

0.40	Fluvoxamine	COVID-19	100,000,000	$690 (Molnupiravir: $700)	0.4: RCT Emulation	$69 billion	Fluvoxamine for COVID-19


Pay-for-Success (PFS) Funding Validation of Affordable Therapies
What is Pay-for-Success (PFS)?
Pay-for-Success (PFS) is an innovative funding model where payments for validating or adopting therapies are contingent on achieving predefined health outcomes. This outcome-driven approach ensures that resources are allocated only to therapies that demonstrate measurable success, making it a powerful tool for driving cost-effective healthcare solutions.
Affordable’s PFS Model
Affordable leverages its ecosystem and $AFF tokens to establish and manage PFS contracts for validating affordable therapies. Using blockchain technology and decentralized science (DeSci) principles, Affordable creates a transparent and reward-driven framework where:
1.	Therapies are Selected: Low-cost therapies with high potential for health impact are identified for validation.
2.	PFS Contracts are Created: Healthcare payers, impact investors, or other stakeholders commit funding or resources based on predefined success metrics (e.g., remission rates, progression-free survival).
3.	Token-Based Rewards:
o	Participants in PFS agreements, including researchers, data contributors, and validators, are rewarded with $AFF tokens which can be converted to fiat. 
o	PFS contract milestones can trigger token distributions to incentivize and reward successful validation efforts.
4.	Validation with Cureledger: Clinical trials are conducted with Contract Research Organization (CRO) partners and data and outcomes are immutably recorded on-chain, ensuring transparency and accountability throughout the PFS process.
Benefits of Using $AFF Tokens in PFS Contracts
•	Global Accessibility: $AFF tokens provide a decentralized and borderless means of incentivizing participation in PFS contracts.
•	Alignment of Incentives: Tokens reward only validated contributions, ensuring that resources are allocated effectively.
•	Scalability: Token-based rewards lower barriers to entry for global collaborators and stakeholders, expanding the reach of PFS agreements.
Affordable Table for PFS Validation
The PFS Affordable Table ranks therapies based on:
•	Outcome Predictability: The likelihood of achieving predefined success metrics.
•	Cost Impact: The potential savings to healthcare systems.
•	Global Applicability: The number of patients who could benefit from the validated therapy.


Affordable PFS Table 
This table focuses on therapies with potential for QALY (Quality-Adjusted Life Year) impact, using evidence probability and global health needs to rank their health benefits. Unlike the IVPE table, this focuses on therapies without high-cost comparators where PFS contracts incentivize the funding of clinical studies and where QALY improvements drive funding priorities.
 
Affordable Algorithm Ranking	Therapy	Disease	Annual Cost ($)	Evidence Probability (Score: Evidence Type)	Global Patient Population	Estimated QALY Impact (Millions)	PubMed Link
0.70	Metformin	Anti-aging, cancer prevention, cardiovascular	$48–$100	0.7: Observational Studies	50,000,000	17.5	MILES Study

0.50	Rapamycin	Cancer prevention, longevity	$48,500	0.4: RCT Emulation	50,000,000	10.0	Rapamycin in Aging

0.40	Paricalcitol	Pancreatic cancer, fibrosis modulation	$1,000–$2,000	0.4: Retrospective Studies	500,000	0.2	Paricalcitol in Pancreatic Cancer

0.35	Ulipristal Acetate (UPA)	Long-term uterine fibroid management	$500–$1,000	0.35: Limited Trials	10,000,000	3.5	UPA for Fibroids

0.65	Ketogenic Diet	Cancer, type 2 diabetes, neurological disorders	$500–$2,000	0.5: Prospective Studies	1,500,000	0.75	Ketogenic Diet for Cancer

0.55	Low-Dose Naltrexone	Autoimmune diseases, chronic pain, fibromyalgia	$30–$50	0.4: RCT Emulation	20,000,000	8.0	LDN in Inflammation

0.50	Aspirin (Low Dose)	Cancer prevention, Alzheimer's, pre-eclampsia	$25–$50	0.5: Prospective Studies	10,000,000	2.5	Aspirin in Cancer Prevention

 
Tokenomics
Affordable tokens ($AFF) are designed to incentivize collaboration and fund research.
Allocation	Percentage	Details
Team	10%	Reserved for core contributors and future hiring.
Public Sale	23%	Open to all participants for decentralized funding.
Pre-Sale	7%	Early supporters and contributors.
Treasury	60%	For funding research, community incentives, and reserves.
 
Community Involvement
Affordable’s global community is at the heart of its mission:
•	Identify Affordable Therapies: Earn token bounties for identifying cost-effective treatments and PubMed links for clinical validation and for supporting estimates of cost-savings for Affordable IVPE candidates or QALY impact for PFS candidates. 
•	Participate in Research: Fund or conduct RCT emulations and validation studies working with CROs and data analytics companies. 
•	Earn Tokens: Receive $AFF for participation and contributions. 

 
A Vision for Public Good Medicines
Affordable embodies the principles outlined in our founder’s LLM thesis, a modified version of the Health Impact Fund. By incentivizing the development and validation of cost-saving treatments, which can subsequently be validated by health insurers in self-funding IVPE trials and PFS mechanisms via CrowdFundedCures.org and PublicGoodPharma.com, Affordable creates a sustainable ecosystem that prioritizes public health over profit. 
Together, we can transform global healthcare, one affordable medicine at a time.
 
Ethical and Medical Disclaimer
Affordable does not make medical claims or endorse specific treatments. All information provided is for research purposes only. Patients and healthcare providers should consult medical professionals before making any treatment decisions. Affordable acts solely as a platform for funding and incentivizing clinical validation of affordable public good medicines. 
V1.1 (23 January 2025) 

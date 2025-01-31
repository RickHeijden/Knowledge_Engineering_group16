# Amazing group 16
**What are characteristics of best-selling books based on their attributes?**

## Current information per data type
### Book
Current information:
- ISBN
- Title
- Author
- Year
- Rating

Not yet available information:
- Publisher (are some publishers more popular than others?)
- Published date (does the time of year affect the rating?)

### Author
- Name
- Birthdate
- Birthplace (is this needed?)
- Birth country
- Death date
- Genres (list of genres the author writes in)
- Influenced
- Influenced by

Not yet available information:
- Gender (gender bias?)

## Interesting questions
- Are some genres more popular in certain years? E.g. more escapism during covid?
- Can we identify trends based on published years?
- Do authors who write in multiple genres have a higher rating?
- Does an author have a higher chance of publishing a best-selling book if they are influenced by a well-known author?
- Does an author have a higher chance of publishing a best-seller later in his/her career?
- How many authors have multiple best-selling books?
- Are there any authors who have a best-selling book in multiple genres?
- Which genres contain the most best-selling books?

## Countries + Nationalities 
country_nationalities = {
    "Afghanistan": "Afghan",
    "Albania": "Albanian",
    "Algeria": "Algerian",
    "Andorra": "Andorran",
    "Angola": "Angolan",
    "Antigua and Barbuda": "Antiguan, Barbudan",
    "Argentina": "Argentine",
    "Armenia": "Armenian",
    "Australia": "Australian",
    "Austria": "Austrian",
    "Azerbaijan": "Azerbaijani",
    "Bahamas": "Bahamian",
    "Bahrain": "Bahraini",
    "Bangladesh": "Bangladeshi",
    "Barbados": "Barbadian",
    "Belarus": "Belarusian",
    "Belgium": "Belgian",
    "Belize": "Belizean",
    "Benin": "Beninese",
    "Bhutan": "Bhutanese",
    "Bolivia": "Bolivian",
    "Bosnia and Herzegovina": "Bosnian, Herzegovinian",
    "Botswana": "Botswanan",
    "Brazil": "Brazilian",
    "Brunei": "Bruneian",
    "Bulgaria": "Bulgarian",
    "Burkina Faso": "Burkinabe",
    "Burundi": "Burundian",
    "Cabo Verde": "Cape Verdian",
    "Cambodia": "Cambodian",
    "Cameroon": "Cameroonian",
    "Canada": "Canadian",
    "Central African Republic": "Central African",
    "Chad": "Chadian",
    "Chile": "Chilean",
    "China": "Chinese",
    "Colombia": "Colombian",
    "Comoros": "Comoran",
    "Congo": "Congolese",
    "Costa Rica": "Costa Rican",
    "Croatia": "Croatian",
    "Cuba": "Cuban",
    "Cyprus": "Cypriot",
    "Czechia": "Czech",
    "Denmark": "Danish",
    "Djibouti": "Djibouti",
    "Dominica": "Dominican",
    "Dominican Republic": "Dominican",
    "East Timor": "East Timorese",
    "Ecuador": "Ecuadorean",
    "Egypt": "Egyptian",
    "El Salvador": "Salvadoran",
    "Equatorial Guinea": "Equatorial Guinean",
    "Eritrea": "Eritrean",
    "Estonia": "Estonian",
    "Eswatini": "Swazi",
    "Ethiopia": "Ethiopian",
    "Fiji": "Fijian",
    "Finland": "Finnish",
    "France": "French",
    "Gabon": "Gabonese",
    "Gambia": "Gambian",
    "Georgia": "Georgian",
    "Germany": "German",
    "Ghana": "Ghanaian",
    "Greece": "Greek",
    "Grenada": "Grenadian",
    "Guatemala": "Guatemalan",
    "Guinea": "Guinean",
    "Guinea-Bissau": "Guinea-Bissauan",
    "Guyana": "Guyanese",
    "Haiti": "Haitian",
    "Honduras": "Honduran",
    "Hungary": "Hungarian",
    "Iceland": "Icelander",
    "India": "Indian",
    "Indonesia": "Indonesian",
    "Iran": "Iranian",
    "Iraq": "Iraqi",
    "Ireland": "Irish",
    "Israel": "Israeli",
    "Italy": "Italian",
    "Jamaica": "Jamaican",
    "Japan": "Japanese",
    "Jordan": "Jordanian",
    "Kazakhstan": "Kazakhstani",
    "Kenya": "Kenyan",
    "Kiribati": "I-Kiribati",
    "Korea, North": "North Korean",
    "Korea, South": "South Korean",
    "Kosovo": "Kosovar",
    "Kuwait": "Kuwaiti",
    "Kyrgyzstan": "Kyrgyzstani",
    "Laos": "Laotian",
    "Latvia": "Latvian",
    "Lebanon": "Lebanese",
    "Lesotho": "Mosotho",
    "Liberia": "Liberian",
    "Libya": "Libyan",
    "Liechtenstein": "Liechtensteiner",
    "Lithuania": "Lithuanian",
    "Luxembourg": "Luxembourger",
    "Madagascar": "Malagasy",
    "Malawi": "Malawian",
    "Malaysia": "Malaysian",
    "Maldives": "Maldivian",
    "Mali": "Malian",
    "Malta": "Maltese",
    "Marshall Islands": "Marshallese",
    "Mauritania": "Mauritanian",
    "Mauritius": "Mauritian",
    "Mexico": "Mexican",
    "Micronesia": "Micronesian",
    "Moldova": "Moldovan",
    "Monaco": "Monacan",
    "Mongolia": "Mongolian",
    "Montenegro": "Montenegrin",
    "Morocco": "Moroccan",
    "Mozambique": "Mozambican",
    "Myanmar": "Burmese",
    "Namibia": "Namibian",
    "Nauru": "Nauruan",
    "Nepal": "Nepali",
    "Netherlands": "Dutch",
    "New Zealand": "New Zealander",
    "Nicaragua": "Nicaraguan",
    "Niger": "Nigerien",
    "Nigeria": "Nigerian",
    "North Macedonia": "Macedonian",
    "Norway": "Norwegian",
    "Oman": "Omani",
    "Pakistan": "Pakistani",
    "Palau": "Palauan",
    "Palestine": "Palestinian",
    "Panama": "Panamanian",
    "Papua New Guinea": "Papua New Guinean",
    "Paraguay": "Paraguayan",
    "Peru": "Peruvian",
    "Philippines": "Filipino",
    "Poland": "Polish",
    "Portugal": "Portuguese",
    "Qatar": "Qatari",
    "Romania": "Romanian",
    "Russia": "Russian",
    "Rwanda": "Rwandan",
    "Saint Kitts and Nevis": "Kittitian, Nevisian",
    "Saint Lucia": "Saint Lucian",
    "Saint Vincent and the Grenadines": "Saint Vincentian, Vincentian",
    "Samoa": "Samoan",
    "San Marino": "Sammarinese",
    "Sao Tome and Principe": "Sao Tomean",
    "Saudi Arabia": "Saudi",
    "Senegal": "Senegalese",
    "Serbia": "Serbian",
    "Seychelles": "Seychellois",
    "Sierra Leone": "Sierra Leonean",
    "Singapore": "Singaporean",
    "Slovakia": "Slovak",
    "Slovenia": "Slovene",
    "Solomon Islands": "Solomon Islander",
    "Somalia": "Somali",
    "South Africa": "South African",
    "South Sudan": "South Sudanese",
    "Spain": "Spanish",
    "Sri Lanka": "Sri Lankan",
    "Sudan": "Sudanese",
    "Suriname": "Surinamer",
    "Sweden": "Swedish",
    "Switzerland": "Swiss",
    "Syria": "Syrian",
    "Taiwan": "Taiwanese",
    "Tajikistan": "Tajikistani",
    "Tanzania": "Tanzanian",
    "Thailand": "Thai",
    "Togo": "Togolese",
    "Tonga": "Tongan",
    "Trinidad and Tobago": "Trinidadian, Tobagonian",
    "Tunisia": "Tunisian",
    "Turkey": "Turkish",
    "Turkmenistan": "Turkmen",
    "Tuvalu": "Tuvaluan",
    "Uganda": "Ugandan",
    "Ukraine": "Ukrainian",
    "United Arab Emirates": "Emirati",
    "United Kingdom": "British",
    "United States": "American",
    "Uruguay": "Uruguayan",
    "Uzbekistan": "Uzbekistani",
    "Vanuatu": "Ni-Vanuatu",
    "Vatican City": "Vatican",
    "Venezuela": "Venezuelan",
    "Vietnam": "Vietnamese",
    "Yemen": "Yemeni",
    "Zambia": "Zambian",
    "Zimbabwe": "Zimbabwean"
}

## Generating the datasets from scratch
### Initially
 - Have a directory called 'datasets'
 - Download the datasets from 
   - https://www.kaggle.com/datasets/dhruvildave/new-york-times-best-sellers/data
   - https://www.kaggle.com/datasets/cmenca/new-york-times-hardcover-fiction-best-sellers
   - https://www.kaggle.com/datasets/jiyoungkimpf/amazon-best-sellers-of-20102020-top-100-books
   - https://www.kaggle.com/datasets/joebeachcapital/amazon-books
 - Unzip the files and place them in the 'datasets' directory

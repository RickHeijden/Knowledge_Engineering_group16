# Dictionary mapping registration group elements to countries
isbn_country_map = {
    '600': 'Iran',
    '601': 'Kazakhstan',
    '602': 'Indonesia',
    '603': 'Saudi Arabia',
    '604': 'Vietnam',
    '605': 'Turkey',
    '606': 'Romania',
    '607': 'Mexico',
    '608': 'North Macedonia',
    '609': 'Lithuania',
    '611': 'Thailand',
    '612': 'Peru',
    '613': 'Mauritius',
    '614': 'Lebanon',
    '615': 'Hungary',
    '616': 'Thailand',
    '617': 'Ukraine',
    '618': 'Greece',
    '619': 'Bulgaria',
    '620': 'Mauritius',
    '621': 'Philippines',
    '622': 'Iran',
    '623': 'Indonesia',
    '624': 'Sri Lanka',
    '625': 'Brazil',
    '65': 'Brazil',
    '80': 'Czech Republic',
    '81': 'India',
    '82': 'Norway',
    '83': 'Poland',
    '84': 'Spain',
    '85': 'Brazil',
    '86': 'former Yugoslavia',
    '87': 'Denmark',
    '88': 'Italy',
    '89': 'Korea',
    '90': 'Netherlands',
    '91': 'Sweden',
    '92': 'International NGO Publishers and EU organizations',
    '93': 'India',
    '94': 'Netherlands',
    '950': 'Argentina',
    '951': 'Finland',
    '952': 'Finland',
    '953': 'Croatia',
    '954': 'Bulgaria',
    '955': 'Sri Lanka',
    '956': 'Chile',
    '957': 'Taiwan',
    '958': 'Colombia',
    '959': 'Cuba',
    '960': 'Greece',
    '961': 'Slovenia',
    '962': 'Hong Kong',
    '963': 'Hungary',
    '964': 'Iran',
    '965': 'Israel',
    '966': 'Ukraine',
    '967': 'Malaysia',
    '968': 'Mexico',
    '969': 'Pakistan',
    '970': 'Mexico',
    '971': 'Philippines',
    '972': 'Portugal',
    '973': 'Romania',
    '974': 'Thailand',
    '975': 'Turkey',
    '976': 'CARICOM',
    '977': 'Egypt',
    '978': 'Nigeria',
    '979': 'Indonesia',
    '980': 'Venezuela',
    '981': 'Singapore',
    '982': 'South Pacific',
    '983': 'Malaysia',
    '984': 'Bangladesh',
    '985': 'Belarus',
    '986': 'Taiwan',
    '987': 'Argentina',
    '988': 'Hong Kong',
    '989': 'Portugal',
    '9915': 'Uruguay',
    '9916': 'Estonia',
    '9917': 'Bolivia',
    '9918': 'Malta',
    '9919': 'Mongolia',
    '9920': 'Morocco',
    '9921': 'Kuwait',
    '9922': 'Iraq',
    '9923': 'Jordan',
    '9924': 'Cambodia',
    '9925': 'Cyprus',
    '9926': 'Bosnia and Herzegovina',
    '9927': 'Qatar',
    '9928': 'Albania',
    '9929': 'Guatemala',
    '9930': 'Costa Rica',
    '9931': 'Algeria',
    '9932': 'Laos',
    '9933': 'Syria',
    '9934': 'Latvia',
    '9935': 'Iceland',
    '9936': 'Afghanistan',
    '9937': 'Nepal',
    '9938': 'Tunisia',
    '9939': 'Armenia',
    '9940': 'Montenegro',
    '9941': 'Georgia',
    '9942': 'Ecuador',
    '9943': 'Uzbekistan',
    '9944': 'Turkey',
    '9945': 'Dominican Republic',
    '9946': 'North Korea',
    '9947': 'Algeria',
    '9948': 'United Arab Emirates',
    '9949': 'Estonia',
    '9950': 'Palestine',
    '9951': 'Kosova',
    '9952': 'Azerbaijan',
    '9953': 'Lebanon',
    '9954': 'Morocco',
    '9955': 'Lithuania',
    '9956': 'Cameroon',
    '9957': 'Jordan',
    '9958': 'Bosnia and Herzegovina',
    '9959': 'Libya',
    '9960': 'Saudi Arabia',
    '9961': 'Algeria',
    '9962': 'Panama',
    '9963': 'Cyprus',
    '9964': 'Ghana',
    '9965': 'Kazakhstan',
    '9966': 'Kenya',
    '9967': 'Kyrgyzstan',
    '9968': 'Costa Rica',
    '9970': 'Uganda',
    '9971': 'Singapore',
    '9972': 'Peru',
    '9973': 'Tunisia',
    '9974': 'Uruguay',
    '9975': 'Moldova',
    '9976': 'Tanzania',
    '9977': 'Costa Rica',
    '9978': 'Ecuador',
    '9979': 'Iceland',
    '9980': 'Papua New Guinea',
    '9981': 'Morocco',
    '9982': 'Zambia',
    '9983': 'Gambia',
    '9984': 'Latvia',
    '9985': 'Estonia',
    '9986': 'Lithuania',
    '9987': 'Tanzania',
    '9988': 'Ghana',
    '9989': 'Macedonia',
    '99901': 'Bahrain',
    '99902': 'Reserved Agency',
    '99903': 'Mauritius',
    '99904': 'Curaçao',
    '99905': 'Bolivia',
    '99906': 'Kuwait',
    '99908': 'Malawi',
    '99909': 'Malta',
    '99910': 'Sierra Leone',
    '99911': 'Lesotho',
    '99912': 'Botswana',
    '99913': 'Andorra',
    '99914': 'Suriname',
    '99915': 'Maldives',
    '99916': 'Namibia',
    '99917': 'Brunei Darussalam',
    '99918': 'Faroe Islands',
    '99919': 'Benin',
    '99920': 'Andorra',
    '99921': 'Qatar',
    '99922': 'Guatemala',
    '99923': 'El Salvador',
    '99924': 'Nicaragua',
    '99925': 'Paraguay',
    '99926': 'Honduras',
    '99927': 'Albania',
    '99928': 'Georgia',
    '99929': 'Mongolia',
    '99930': 'Armenia',
    '99931': 'Seychelles',
    '99932': 'Malta',
    '99933': 'Nepal',
    '99934': 'Dominican Republic',
    '99935': 'Haiti',
    '99936': 'Bhutan',
    '99937': 'Macau',
    '99938': 'Republika Srpska',
    '99939': 'Guatemala',
    '99940': 'Georgia',
    '99941': 'Armenia',
    '99942': 'Sudan',
    '99943': 'Albania',
    '99944': 'Ethiopia',
    '99945': 'Namibia',
    '99946': 'Nepal',
    '99947': 'Tajikistan',
    '99948': 'Eritrea',
    '99949': 'Mauritius',
    '99950': 'Cambodia',
    '99951': 'Reserved Agency',
    '99952': 'Mali',
    '99953': 'Paraguay',
    '99954': 'Bolivia',
    '99955': 'Republika Srpska',
    '99956': 'Albania',
    '99957': 'Malta',
    '99958': 'Bahrain',
    '99959': 'Luxembourg',
    '99960': 'Malawi',
    '99961': 'El Salvador',
    '99962': 'Mongolia',
    '99963': 'Cambodia',
    '99964': 'Nicaragua',
    '99965': 'Macau',
    '99966': 'Kuwait',
    '99967': 'Paraguay',
    '99968': 'Botswana',
    '99969': 'Oman',
    '99970': 'Haiti',
    '99971': 'Myanmar',
    '99972': 'Faroe Islands',
    '99973': 'Mongolia',
    '99974': 'Bolivia',
    '99975': 'Tajikistan',
    '99976': 'Republika Srpska',
    '99977': 'Rwanda',
    '99978': 'Mongolia',
    '99979': 'Honduras',
    '99980': 'Bhutan',
    '99981': 'Macau',
    '99982': 'Benin',
    '99983': 'El Salvador'
}

# Dictionary mapping registration group elements to language areas
language_group_map = {
    '0': 'English-speaking area',
    '1': 'English-speaking area',
    '2': 'French-speaking area',
    '3': 'German-speaking area',
    '4': 'Japan',
    '5': 'Russia',
    '7': 'China'
}


def get_country_from_isbn(isbn):
    if not type(isbn) == str:
        isbn = str(isbn)
    if isbn.startswith('978'):
        isbn_prefix = isbn[3:]
    elif isbn.startswith('979'):
        isbn_prefix = isbn[3:]
    else:
        print('ISBN must start with 978 or 979')
        return None

    # First, try to find a direct match in the isbn_country_map
    for length in range(5, 0, -1):
        if isbn_prefix[:length] in isbn_country_map:
            return isbn_country_map[isbn_prefix[:length]]

    # If no direct match is found, use the first digit after the prefix for a general language group
    first_digit = isbn_prefix[0]
    return language_group_map.get(first_digit, 'Unknown country')

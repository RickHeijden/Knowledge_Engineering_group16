import pandas as pd
import re


def clean_authors(authors_series: pd.Series):
    """
    Cleans the authors in the authors_series by removing special characters, titles, and other unwanted strings.

    @param authors_series: The series of authors to clean.

    @return: The cleaned authors.
    """
    return authors_series.apply(cleaning_authors).apply(split_authors).apply(formalize_initials)


def cleaning_authors(author: str) -> str:
    """
    Cleans the author string by removing special characters, titles, and other unwanted strings.

    @param author: The author string to clean.

    @return: The cleaned author string.
    """
    if pd.isnull(author):
        return author

    # Replace special characters with normal ones
    author = author.replace('’', "'")
    author = author.replace('–', '-')
    author = author.replace('“', '"')
    author = re.sub(r'\u2009', ' ', author)

    # Remove any 'PhD' or 'Ph.D.' (case-insensitive)
    author = re.sub(r'(?i)\bph\.?d\.?\b', '', author)

    # Remove any 'MD' or 'M.D.' or 'M.Ed. (case-insensitive)
    author = re.sub(r'(?i)\bm\.?e?d\.?\b', '', author)

    # Remove any 'FACLM' (case-insensitive)
    author = re.sub(r'(?i)faclm', '', author)

    # Remove any 'Dr' or 'Dr.' (case-insensitive)
    if author.lower() != 'dr. seuss':
        author = re.sub(r'(?i)\bdr\.?\b', '', author)

    # Remove any 'by' (case-insensitive)
    author = re.sub(r'(?i)\bby\b', '', author)

    # Remove any 'x more' where x is an integer (case-insensitive)
    author = re.sub(r'(?i)\b\d+\s*more\b', '', author)

    # Remove any 'MS' (case-insensitive)
    author = re.sub(r'(?i)\bms\b', '', author)

    # Remove any 'RD' (case-insensitive)
    author = re.sub(r'(?i)\brd\b', '', author)

    # Remove any 'CDN' (case-insensitive)
    author = re.sub(r'(?i)\bcdn\b', '', author)

    # Remove any 'et al' or 'et al.' (case-insensitive)
    author = re.sub(r'(?i)\bet\s+al\.?\b', '', author)

    # Changes strings with things like '(Narrator, Author)' to '(Author)'
    author = re.sub(r'\(\w+,\s+Author\)', '(Author)', author)

    # Remove any 'Publisher' or 'Editor' or 'Illustrator' or 'Compiler', but not 'Author' and 'Narrator'
    author = ",".join([
        s for s in author.split(',')
        if ((('Publisher' not in s) and
            ('Editor' not in s) and
            ('Illustrator' not in s) and
            ('Compiler' not in s))
            or 'Author' in s or 'Narrator' in s)
    ])
    author = ";".join([
        s for s in author.split(';')
        if ((('Publisher' not in s) and
            ('Editor' not in s) and
            ('Illustrator' not in s) and
            ('Compiler' not in s))
            or 'Author' in s or 'Narrator' in s)
    ])

    # Remove any (x,) with (,) optional, where x is 'Author' or 'Narrator'
    author = re.sub(r'\(?(Author|Narrator),?\)?', '', author)

    # Handle special cases with the publisher DK
    author = (author
              .replace('Stan LeeDK', 'Stan Lee;DK')
              .replace('Stephen WiacekDKStan Lee', 'Stephen Wiacek;DK;Stan Lee'))

    # Remove any 'Format: ' followed by any word characters and whitespace
    author = re.sub(r'(?i)Format:[\s\w]+', '', author)

    # Remove any '(Foreword)' (case-insensitive)
    author = re.sub(r'(?i)\(Foreword\)', '', author)

    # Remove any '- foreword', '- essay', etc (case-insensitive)
    author = re.sub(r'(?i)- (introduction|foreword|essay|translator|abridgement( and introduction)?)', '', author)

    # Replace any 'created' by ';' in the middle (case-insensitive)
    author = re.sub(r'(?i)\bcreated\b', ';', author)

    # Remove any 'edited' or 'written', etc (case-insensitive), yes illlustrated is with 3 l's
    author = re.sub(
        r'(?i)(edited|written|adapted|created|lyrics|from texts|novelization|compiled|admiral|introduced|selected|illlustrated|Illustrations|from PopularMMOs)',
        '',
        author,
    )

    # Remove any 'various authors' or 'various illustrators' or 'various authors and artists' (case-insensitive)
    author = re.sub(r'(?i)various\s?(authors( and artists)?|illustrators)?', '', author)

    # Remove any 'with an introduction' or 'with related materials' (case-insensitive)
    author = re.sub(r'(?i)with (an introduction|related materials)', '', author)

    # Remove any 'and others', 'with others', '& [some number] others' (case-insensitive)
    author = re.sub(r'(?i)(and|with|(& )?\d+)?\s*others', '', author)

    # Remove any 'writing as' with any word characters and whitespace preceding (case-insensitive)
    author = re.sub(r'(?i)[\w\s]+writing as', '', author)

    # Replace any 'as told to' by ';' in the middle (case-insensitive)
    author = re.sub(r'(?i)as told to', ';', author)

    # Remove any 'photographs, 'translated', or 'designed' with any word characters and whitespace following
    #  (case-insensitive)
    author = re.sub(r'(?i)(photographs|translated|designed)[\w\s]+', '', author)

    # Remove any leading or trailing whitespace and punctuation
    author = author.strip().strip('.,&;').strip()

    # This puts a semicolon between two authors that are concatenated
    str_author = str(author)
    for author_split in re.split(r'\.|\s|-|;', str_author):
        author_split = author_split.strip('.,&();"\'')

        # Skip the exceptions
        exceptions = [
            'PewDiePie',
            'MaryJanice',
            'RPG',
            'MALCOLM',
            'VTech',
            'KEI',
            'HILL',
            'ERIC',
            'PopularMMOs',
            'NOFX',
            'MarcyKate',
            'NaRae',
            'JoJo',
            'HARARI',
            'YUVAL',
            'NOAH',
            'WuDunn',
            'BarkPost',
            'JoAnn',
            'deGrasse',
            'MantraCraft',
            'Oh!Great',
            'HighBridge',
            'OKAYADO',
            'StacyPlays',
            'FitzSimmons',
            'ACT',
            'GMAC',
            'LevelFiveMedia',
            'III',
            'WEB',
            'JRR',
            'RaeAnne',
            'ONE',
            'TEAS',
            'QuinRose',
            'MonRin',
            'RoseMarie',
        ]
        if any(author_split == exception for exception in exceptions):
            continue

        # Remove any 'Mac' or 'Mc' or 'Dell' or 'Di'/'De'/'Du' or "O/D/L'" or 'Ter' or 'La' (case-insensitive)
        author_split = re.sub(r'M(a)?c', '', author_split)
        author_split = re.sub(r"Dell'", '', author_split)
        author_split = re.sub(r'D[ieu]', '', author_split)
        author_split = re.sub(r"[ODL]'", '', author_split)
        author_split = re.sub(r"Ter", '', author_split)
        author_split = re.sub(r"La", '', author_split)

        if len(author_split) > 2:
            author_split_without_first = author_split[1:]
            if any(c.isupper() for c in author_split_without_first):
                # Put semicolon between the authors, before the letter in uppercase in code not print
                author = author.replace(
                    author_split_without_first,
                    "".join([';' + char if char.isupper() else char for char in author_split_without_first])
                )

    return author


def split_authors(author: str, words_to_split_author: list[str | None] | None = None) -> str:
    """
    Splits the author string by the words in words_to_split_author.

    @param author: The author string to split.
    @param words_to_split_author: The words to split the author string by.
    @return: The author string split by the words in words_to_split_author.
    """
    if pd.isnull(author):
        return author

    if words_to_split_author is None:
        words_to_split_author = [' and ', ',', '&', ';with', ' with ', ';']

    # Start with the original author string
    authors = [author]

    # Split the author string by each word in words_to_split_author
    for word in words_to_split_author:
        authors = [a for auth in authors for a in auth.split(word)]

    # Join the resulting list with ';'
    return ';'.join([a.strip().strip('.,&;').strip() for a in authors if a.strip() != ''])


def formalize_initials(author: str) -> str:
    """
    Formalizes the initials of the author string by making sure that each initial is followed by a dot and a space.

    @param author: The author string to formalize the initials of.
    @return: The author string with formalized initials.
    """
    if pd.isnull(author):
        return author

    # Find any pattern that matches initials of length up to 3
    # An initial is a capital letter with or without a dot
    #  preceded by a space or a word boundary and followed by a space
    # The initials are followed up by a last name, which is a word of at least 2 letters.
    results = re.finditer(r'\b[A-Z]\.?[\s\b]*([A-Z]\.?)?[\s\b]*([A-Z]\.?)?[\s\b]+\w{2,}', author)
    # We will replace initials to make them conform, but this shifts the string, we keep track of this
    start_diff = 0

    for result in results:
        # DK/RPG are publishers that falls under this pattern, so we skip it.
        if not result.group().startswith('DK ') and not result.group().startswith('RPG'):
            # Finding the last name of the author
            name = re.search(r'[\s\b]+\w{2,}', result.group())

            # Finding the positions of the initials of the author
            start_initials = result.start() + start_diff
            end_initials = start_initials + name.start()

            # Extracting the initials and making it such that each initial is followed by a dot and a space
            #  we make sure that first dots and spaces are removed to avoid duplications of dots and spaces
            initials = author[start_initials:end_initials].replace('.', '')
            initials = re.sub(r'[\s\b]+', '', initials)
            initials = re.sub(r'[A-Z]', (lambda x: x.group() + '. '), initials).strip()

            # Adjust the starting difference by the change in length of the initials
            start_diff += len(initials) - (end_initials - start_initials)

            # Replace the initials in the author string
            author = author[:start_initials] + initials + author[end_initials:]

    return author

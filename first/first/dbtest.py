import sqlite3
import operator


def is_page_exist(url_f):
    with sqlite3.connect('db.db') as conn:
        cursor = conn.execute("SELECT url FROM pages WHERE url=?", (url_f,))
        row = cursor.fetchone()
        if row is None:
            return False
        else:
            return True


def store_page(url_f, title_f, content_f):
    # page = Page(url, title, content)
    with sqlite3.connect('db.db') as conn:
        conn.execute("INSERT INTO pages (url, title, content)"
                     "VALUES (?, ?, ?)", (url_f, title_f, content_f,))
        conn.commit()
        return "done"


# print is_page_exist("https://www.apple.com")

# print store_page("https://yahoo.com", "Hello", "Hello to everyone")

def is_word_exist(word_f):
    with sqlite3.connect('db.db') as conn:
        cursor = conn.execute("SELECT word FROM words WHERE word=?", (word_f,))
        row = cursor.fetchone()
        if row is None:
            return False
        else:
            return True


def store_word_if_not_exist(text):
    is_exist = is_word_exist(text)
    if not is_exist:
        with sqlite3.connect('db.db') as conn:
            conn.execute("INSERT INTO words (word)"
                         "VALUES (?)", (text,))
            conn.commit()
            return "done"


def get_word_id(text):
    with sqlite3.connect('db.db') as conn:
        cursor = conn.execute("SELECT id, word FROM words WHERE word=(:word)", {"word": text})
        row = cursor.fetchone()
        if row is None:
            return False
        else:
            return row[0]


def get_page_id(url_f):
    with sqlite3.connect('db.db') as conn:
        cursor = conn.execute("SELECT id FROM pages WHERE url=?", (url_f,))
        row = cursor.fetchone()
        if row is None:
            return False
        else:
            return row[0]


def search(word_f, word_not_f):
    word = get_word_id(word_f)
    word_not = get_word_id(word_not_f)
    with sqlite3.connect('db.db') as conn:
        cursor = conn.execute('''SELECT page_id FROM indexes WHERE word_id IN (:word)''', {"word": word})
        rows_and = cursor.fetchall()
        cursor = conn.execute('''SELECT page_id FROM indexes WHERE word_id IN (:word_not)''', {"word_not": word_not})
        rows_not = cursor.fetchall()

        res = []
        for n in rows_and:
            if n not in rows_not:
                res.append(n)
        print res
        # for n in rows_and:
        #	print n in rows_not
        print "\n\n\n"
        print rows_and
        print "\n\n"
        print rows_not
        if rows is None:
            return False
        else:
            for row in rows:
                print row


def search_advance_and(words_and_id_f):
    words = []
    for word_and_id in words_and_id_f:
        with sqlite3.connect('db.db') as conn:
            cursor = conn.execute('''SELECT page_id FROM indexes WHERE word_id IN (:word) ORDER BY score''',
                                  {"word": word_and_id})
            rows = cursor.fetchall()
            words.append(rows)

    words_len = len(words)

    # for word in words:
    #	print word
    #	print "\n\n\n"
    result = set(words[0])
    for cnt in range(1, words_len):
        result = set(words[cnt]) & result
    result = list(result)
    result.sort()
    return result


def search_advance_or(words_or_id_f):
    words = []
    for word_or_id in words_or_id_f:
        with sqlite3.connect('db.db') as conn:
            cursor = conn.execute('''SELECT page_id FROM indexes WHERE word_id IN (:word) ORDER BY score''',
                                  {"word": word_or_id})
            rows = cursor.fetchall()
            words.append(rows)
    result = []
    for word in words:
        result += word
    result = list(set(result))
    result.sort()
    return result


# for word in words:
# return result

def search_advance_not(words_not_id_f):
    words = []
    for word_not_id in words_not_id_f:
        with sqlite3.connect('db.db') as conn:
            cursor = conn.execute('''SELECT page_id FROM indexes WHERE word_id IN (:word) ORDER BY score''',
                                  {"word": word_not_id})
            rows = cursor.fetchall()
            words.append(rows)
    result = []
    for word in words:
        result += word
    result = list(set(result))
    result.sort()
    return result


def search_advance(words_and_f, words_or_f, words_not_f):
    words_and_f = words_and_f.split(' ')
    words_or_f = words_or_f.split(' ')
    words_not_f = words_not_f.split(' ')

    words_and_id = []
    words_or_id = []
    words_not_id = []

    for word in words_and_f:
        words_and_id.append(get_word_id(word))
    for word in words_or_f:
        words_or_id.append(get_word_id(word))
    for word in words_not_f:
        words_not_id.append(get_word_id(word))

    and_result = search_advance_and(words_and_id)
    or_result = search_advance_or(words_or_id)
    not_result = search_advance_not(words_not_id)

    ('\n'
     '	print and_result\n'
     '	print "\n\n\n"\n'
     '\n'
     '	print or_result\n'
     '	print "\n\n\n"\n'
     '\n'
     '	print not_result\n'
     '	print "\n\n\n"\n'
     '	')
    and_or_result_set = and_result + or_result
    and_or_result_set = set(and_or_result_set)

    # x = list(and_or_result_set)
    # x.sort()
    # print x
    # print "\n\n\n"
    not_result_set = set(not_result)
    page_results = and_or_result_set - not_result_set
    page_results = list(page_results)
    # print "Main Result is: "
    # print result
    # return page_result
    # print page_results
    main_words_id = words_and_id + words_or_id

    main_score = {}
    score = 0
    for page_result in page_results:
        score = 0
        for main_word_id in main_words_id:
            with sqlite3.connect('db.db') as conn:
                cursor = conn.execute(
                    '''SELECT score FROM indexes WHERE word_id = :main_word AND page_id = :page_result''',
                    {"main_word": main_word_id, "page_result": page_result[0]})
                row = cursor.fetchone()
                if row != None:
                    score += row[0]
        main_score[page_result[0]] = score

    # print main_score
    # print max(main_score.iteritems(), key=operator.itemgetter(1))[0]
    # print sorted(main_score.items(), key=operator.itemgetter(1))
    main_score_sorted = sorted(main_score.items(), key=operator.itemgetter(1))

    last_result = []
    # print main_score_sorted
    main_score_len = len(main_score_sorted)
    for main_cnt in range(-1, -main_score_len - 1, -1):
        with sqlite3.connect('db.db') as conn:
            cursor = conn.execute('''SELECT url, title FROM pages WHERE id = :page_id ''',
                                  {"page_id": main_score_sorted[main_cnt][0]})
            row = cursor.fetchone()
            # print row[0], row[1]
            last_result.append(row)
    return last_result


def search_basic(words_f):
    words = words_f.split(' ')
    words_id = []
    for word in words:
        words_id.append(get_word_id(word))

    first_results = search_advance_and(words_id)

    main_score = {}
    score = 0
    for first_result in first_results:
        score = 0
        for word_id in words_id:
            with sqlite3.connect('db.db') as conn:
                cursor = conn.execute(
                    '''SELECT score FROM indexes WHERE word_id = :main_word AND page_id = :page_result''',
                    {"main_word": word_id, "page_result": first_result[0]})
                row = cursor.fetchone()
                if row != None:
                    score += row[0]
            if first_result[0] in main_score:
                main_score[first_result[0]] += score
            else:
                main_score[first_result[0]] = score

    main_score_sorted = sorted(main_score.items(), key=operator.itemgetter(1))
    last_result = []
    main_score_len = len(main_score_sorted)
    for main_cnt in range(-1, -main_score_len - 1, -1):
        with sqlite3.connect('db.db') as conn:
            cursor = conn.execute('''SELECT url, title FROM pages WHERE id = :page_id ''',
                                  {"page_id": main_score_sorted[main_cnt][0]})
            row = cursor.fetchone()
            # print row[0], row[1]
            last_result.append(row)
    return last_result


def store_history(and_words_f, or_words_f, not_words_f):
    with sqlite3.connect('db.db') as conn:
        conn.execute("INSERT INTO history (and_words, or_words, not_words) VALUES (?, ?, ?)",
                     (and_words_f, or_words_f, not_words_f))
        conn.commit()
    return "done"


def get_history(count_f):
    with sqlite3.connect('db.db') as conn:
        cursor = conn.execute('''SELECT MAX(id) FROM history''')
        max_id = cursor.fetchone()[0]
    with sqlite3.connect('db.db') as conn:
        cursor = conn.execute(
            '''SELECT and_words, or_words, not_words FROM history WHERE id BETWEEN :max_id - :count_f +1 AND :max_id ''',
            {"max_id": max_id, "count_f": count_f})
        rows = cursor.fetchall()
        return rows


#print get_word_id("LANGUAGE")
# print get_history(2)
# print store_history("pythasdasdon a dasdadsaaright", "thsadasdasey a", "nightasad shift")
# print search_basic("python code")
# print search_advance("python right knowledg", "code they return create get", "it")
# print search_advance_not([1,2])
# print search_advance_or([1, 2])
# print search_advance("python right", "dsa", "sdadsasa")
# print search_advance_and([4,5])
# search("python", "knowledg")
# print get_word_id("knowledg")
# print store_word_if_not_exist("salam")
# print store_word_if_not_exist("hi")
# print get_word_id("hi")[0]
# print store_word_if_not_exist("salam")
# print is_word_exist("salam")
# print get_page_id("https://yahoo.com")
# print store_page("www.google.com", "shdaha", "content_f")

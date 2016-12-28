# -*- coding: utf-8 -*-
from flask import Flask, url_for, request, redirect, render_template
import dbtest, dbtest2
import csv
import json
import nltk
from stemming.porter2 import stem
import re

app = Flask(__name__)


def porter(words):
    words = re.findall(r'[A-Za-z0-9]\w*', words.lower())
    for i in range(len(words)):
        words[i] = stem(words[i])
    delWord = dict(nltk.pos_tag(words))
    for i in delWord:
        if delWord[i] == 'DT' or delWord[i] == 'IN' or delWord[i] == 'CC' or delWord[i] == 'TO':
            for j in range(words.count(i)):
                words.remove(i)
    sum = ""
    for cnt in range(len(words) - 1):
        sum += words[cnt] + " "
    sum += words[len(words)-1]
    #print sum
    return sum


@app.route("/")
def index():
    return render_template("main.html")


@app.route("/_req_advance", methods=["post", "get"])
def advance_search():
    and_words = request.form["and"]
    and_words = and_words.lower()
    if(and_words != ""):
        and_words = porter(and_words)
    or_words = request.form["or"]
    or_words = or_words.lower()
    if(or_words != ""):
        or_words = porter(or_words)
    not_words = request.form["not"]
    not_words = not_words.lower()
    if(not_words != ""):
        not_words = porter(not_words)
    dbtest.store_history(and_words, or_words, not_words)
    #print subtitle
    search = request.form['search'] if "search" in request.form else None
    #row = dbtest.get_data(subtitle, search)
    row, time = dbtest2.search_advance(and_words, or_words, not_words)
    return render_template("results_advance.html", results=row, time=time, lenrow=len(row))


@app.route("/results_advance")
def result_advance():
    result1 = request.args.get("results")
    return render_template("results_advance.html", results=result1)


@app.route("/_req_basic", methods=["post", "get"])
def basic_search():
    words = request.form["words"]
    words = words.lower()
    words = porter(words)
    dbtest.store_history(" ", words, " ")
    row, time = dbtest2.search_basic(words)

    return render_template("results_basic.html", results=row, time=time, lenrow=len(row))


@app.route("/results_basic")
def result_basic():
    result1 = request.args.get("results")
    return render_template("results_basic.html", results=result1)


@app.route("/_req_history", methods=["post", "get"])
def history_search():
    #words = request.form["words"]
    history_count = request.form["history_count"]
    row = dbtest.get_history(history_count)
    return render_template("results_history.html", results=row)


@app.route("/results_history")
def result_history():
    result1 = request.args.get("results")
    return render_template("results_history.html", results=result1)

if __name__ == "__main__":
    app.debug = True
    app.run()

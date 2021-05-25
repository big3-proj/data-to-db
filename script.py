# -*- coding= utf-8 -*-
from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
import os
import pandas as pd
import itertools
from ckiptagger import WS, POS
from tqdm import tqdm
import sys
from datetime import datetime

pjdir = os.path.abspath(os.path.dirname(__file__))
# Create a Flask APP
app = Flask(__name__)
app.url_map.strict_slaskes = False
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////' + os.path.join(pjdir, 'data.sqlite')
db = SQLAlchemy(app)


class User(db.Model):
    __tablename__ = 'users'
    uid = db.Column(db.String(12), unique=True, nullable=False, primary_key=True)
    ips = db.Column(db.Text)
    posts = db.relationship('Post', backref='user', lazy='dynamic')
    pushes = db.relationship('Push', backref='user', lazy='dynamic')
    words = db.relationship('Word', backref='user', lazy='dynamic')
    def __init__(self, uid, ip=''):
        self.uid = uid
        self.ips = ip
    def __repr__(self):
        return f'{self.uid}'


class Post(db.Model):
    __tablename__ = 'posts'
    pid = db.Column(db.String(20), unique=True, nullable=False, primary_key=True)
    title = db.Column(db.String(50))
    content = db.Column(db.Text)
    datetime = db.Column(db.DateTime)
    ip = db.Column(db.Text)
    user_id = db.Column(db.String(12), db.ForeignKey('users.uid'))
    pushes = db.relationship('Push', backref='post', lazy='dynamic')
    def __init__(self, pid, title, content, dt, ip=''):
        self.pid = pid
        self.title = title
        self.content = content
        self.datetime = datetime.strptime(dt, '%a %b %d %H:%M:%S %Y')
        self.ip = ip
    def __repr__(self):
        return f'{self.pid}'


class Push(db.Model):
    __tablename__ = 'pushes'
    id = db.Column(db.Integer, primary_key=True)
    tag = db.Column(db.String(1))
    content = db.Column(db.Text())
    datetime = db.Column(db.DateTime)
    floor = db.Column(db.Integer)
    ip = db.Column(db.Text)
    user_id = db.Column(db.String(12), db.ForeignKey('users.uid'))
    post_id = db.Column(db.String(20), db.ForeignKey('posts.pid'))
    def __init__(self, tag, content, dt, floor, ip=''):
        self.tag = tag
        self.content = content
        self.datetime = datetime.strptime(dt, '%Y/%m/%d %H:%M')
        self.floor = floor
        self.ip = ip
    def __repr__(self):
        return f'{self.post_id}, floor: {self.floor}'


class Word(db.Model):
    __tablename__ = 'words'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(12), db.ForeignKey('users.uid'))
    content = db.Column(db.Text())
    pos = db.Column(db.String(20))
    day_count = db.Column(db.Text())
    def __init__(self, content, pos):
        self.content = content
        self.pos = pos
        self.day_count = ','.join(map(str, [0]*367))
    def __repr__(self):
        return f'{self.content}, day_count: {self.day_count}'


def tag_word(content):
    # tagger
    ws_list = ws(content)
    pos_list = pos(ws_list)
    # flatten and merge
    ws_list = list(itertools.chain(*ws_list))
    pos_list = list(itertools.chain(*pos_list))
    wp_list = [[ws_list[i], pos_list[i]] for i in range(len(ws_list))]

    return wp_list


def tag_sentence(day_of_the_year, users_sentences_in_day):
    for uid, sentence in tqdm(users_sentences_in_day.items(), desc=f'Tagging sentence'):
        wp_list = tag_word(sentence)
        for w, p in wp_list:
            word = Word.query.filter_by(content=w, user_id=uid).first()
            if word is None: word = Word(w, p)
            word_day_count = list(map(int, word.day_count.split(',')))
            word_day_count[0] += 1 # sum
            word_day_count[day_of_the_year] += 1
            word.day_count = ','.join(map(str, word_day_count))
            user = User.query.filter_by(uid=uid).first()
            user.words.append(word)
        db.session.add(user)
        db.session.commit()


def parse_data():
    '''
        parse sentences in post, grouping by day & user
    '''
    users_sentences_in_day = {}
    day_of_the_year = -1
    for art in tqdm(data['articles'], desc=f'Parsing articles'):
        try:    # add post to DB
            post = Post(art['article_id'], art['article_title'], art['content'] , art['date'], art['ip'])
        except:
            continue

        # day changed, tag accumulated sentences, update day and reset dictionary
        if day_of_the_year != int(post.datetime.strftime('%j')):
            tag_sentence(day_of_the_year, users_sentences_in_day)
            day_of_the_year = int(post.datetime.strftime('%j'))
            users_sentences_in_day = {}

        # remove author nickname, the rest part is user id
        try:
            author_id = art['author'].split()[0]
        except:
            author_id = ''
        
        author = User.query.filter_by(uid=author_id).first()
        # if user is not exist in DB, create it, otherwise, store the ip address
        if author is None:
            author = User(author_id, art['ip'])
        else:
            # add delimeter between multiple ips
            if author.ips:
                author.ips += ';'
            author.ips += art['ip']

        author.posts.append(post)
        db.session.add(author)
        db.session.commit()
        if author_id not in users_sentences_in_day:
            users_sentences_in_day[author_id] = []
        users_sentences_in_day[author_id].append(art['content'])

        # add pushes to DB
        floor = 0
        for m in art['messages']:
            # parse ipdatetime
            push_ip = ''
            push_datetime = ''
            push_ipdatetime_list = m['push_ipdatetime'].split()
            # check whether the push has ip and datetime or not
            if len(push_ipdatetime_list) == 3: # both ip and datetime
                push_ip = push_ipdatetime_list[0]
                push_datetime = post.datetime.strftime('%Y')+'/'+push_ipdatetime_list[1]+' '+push_ipdatetime_list[2]
            elif len(push_ipdatetime_list) == 2: # only datetime
                # if no time data, set to 00:00
                if len(push_ipdatetime_list[1]) == 1:
                    push_ipdatetime_list[1] = '00:00'
                push_datetime = post.datetime.strftime('%Y')+'/'+push_ipdatetime_list[0]+' '+push_ipdatetime_list[1]
            else: # no datetime data
                # use post's datetime for substitution
                push_datetime = post.datetime.strftime('%Y/%m/%d %H:%M')
            try:
                push = Push(m['push_tag'], m['push_content'], push_datetime, floor, push_ip)
            except:
                print(m['push_ipdatetime'])
            pusher = User.query.filter_by(uid=m['push_userid']).first()
            if pusher is None: pusher = User(m['push_userid'])

            # if the push has ip, add to User
            if push_ip:
                if pusher.ips:
                    pusher.ips += ';'
                pusher.ips += push_ip

            pusher.pushes.append(push)
            post.pushes.append(push)

            if m['push_userid'] not in users_sentences_in_day:
                users_sentences_in_day[m['push_userid']] = []
            users_sentences_in_day[m['push_userid']].append(m['push_content'])
            floor += 1
        
        db.session.add(post)
        db.session.commit()
    tag_sentence(day_of_the_year, users_sentences_in_day)

if __name__ == '__main__':
    if os.path.isfile(f'{sys.argv[1]}'):
        data = pd.read_json(f'{sys.argv[1]}')

        ws = WS('ckipdata')
        pos = POS('ckipdata')

        db.create_all()

        parse_data()

        del ws
        del pos
    else:
        print(f'File {sys.argv[1]} not found.')
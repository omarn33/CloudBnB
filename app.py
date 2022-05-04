from flask import Flask, render_template
import json

app = Flask(__name__)

with open('data.json', 'r', encoding='utf8') as f:
    data = json.load(f)

@ app.route('/')
def home():
    return render_template('home.html')


@ app.route('/listings')
def listings():
    return render_template('listings.html', listings=data)


if __name__ == '__main__':
    app.run(debug=True)

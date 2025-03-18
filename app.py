from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///contacts.db'  # Use SQLite for simplicity
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class Contact(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    phoneNumber = db.Column(db.String, unique=False, nullable=True)
    email = db.Column(db.String, unique=False, nullable=True)
    linkedId = db.Column(db.Integer, db.ForeignKey('contact.id'), nullable=True)
    linkPrecedence = db.Column(db.String, nullable=False)
    createdAt = db.Column(db.DateTime, default=datetime.utcnow)
    updatedAt = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    deletedAt = db.Column(db.DateTime, nullable=True)

def find_primary_contact(email, phoneNumber):
    query = Contact.query.filter((Contact.email == email) | (Contact.phoneNumber == phoneNumber)).all()
    if not query:
        return None, None
    primary_contact = None
    secondary_contacts = []
    
    for contact in query:
        if contact.linkPrecedence == "primary":
            primary_contact = contact
        else:
            secondary_contacts.append(contact)
    
    if primary_contact:
        secondary_contacts.extend([c for c in query if c.linkPrecedence == "secondary" and c.linkedId == primary_contact.id])
        return primary_contact, secondary_contacts
    else:
        primary_contact = query[0]
        return primary_contact, query[1:]

@app.route('/identify', methods=['POST'])
def identify():
    data = request.get_json()
    email = data.get("email")
    phoneNumber = data.get("phoneNumber")

    primary_contact, secondary_contacts = find_primary_contact(email, phoneNumber)

    if primary_contact:
        if (email and email not in [c.email for c in secondary_contacts]) or \
           (phoneNumber and phoneNumber not in [c.phoneNumber for c in secondary_contacts]):
            new_contact = Contact(email=email, phoneNumber=phoneNumber, linkedId=primary_contact.id, linkPrecedence="secondary")
            db.session.add(new_contact)
            db.session.commit()
            secondary_contacts.append(new_contact)

        response = {
            "contact": {
                "primaryContatctId": primary_contact.id,
                "emails": list(set([primary_contact.email] + [c.email for c in secondary_contacts if c.email])),
                "phoneNumbers": list(set([primary_contact.phoneNumber] + [c.phoneNumber for c in secondary_contacts if c.phoneNumber])),
                "secondaryContactIds": [c.id for c in secondary_contacts]
            }
        }
    else:
        new_contact = Contact(email=email, phoneNumber=phoneNumber, linkPrecedence="primary")
        db.session.add(new_contact)
        db.session.commit()
        response = {
            "contact": {
                "primaryContatctId": new_contact.id,
                "emails": [new_contact.email] if new_contact.email else [],
                "phoneNumbers": [new_contact.phoneNumber] if new_contact.phoneNumber else [],
                "secondaryContactIds": []
            }
        }

    return jsonify(response), 200

if __name__ == '__main__':
    with app.app_context():
        db.create_all()  # Create tables before running the app
    app.run(debug=True)

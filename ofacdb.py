
"""
MIT License

Copyright (c) 2016 Sparrow A.I., LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""


# Contact: Scott Condie (scott@sparrowai.com)



from peewee import *
import csv
from elasticsearch import Elasticsearch

database = SqliteDatabase("ofac.db")         # async doesn't seem to work with sqlite3, so synchronous for now

class BaseModel(Model):
    class Meta:
        database = database


class SDN(BaseModel):
    ent_num = IntegerField(unique=True, primary_key=True)
    sdn_name = CharField()
    sdn_type = CharField()
    program = CharField()
    title = CharField()
    call_sign = CharField()
    vess_type = CharField()
    tonnage = CharField()
    grt = CharField()
    vess_flag = CharField()
    vess_owner = CharField()
    remarks = CharField()


class SDNAddress(BaseModel):
    ent_num = IntegerField()
    add_num = IntegerField()
    address = CharField()
    city_state_province_postal  = CharField()
    country = CharField()
    add_remarks = CharField()

class SDNAlternateIdentity(BaseModel):
    ent_num = IntegerField()
    alt_num = IntegerField()
    alt_type = CharField()
    alt_name = CharField()
    alt_remarks = CharField()

class SDNComment(BaseModel):
    ent_num = IntegerField()
    comments = CharField()


class Consolidated(BaseModel):
    ent_num = IntegerField(unique=True)
    sdn_name = CharField()
    sdn_type = CharField()
    program = CharField()
    title = CharField()
    call_sign = CharField()
    vess_type = CharField()
    tonnage = CharField()
    grt = CharField()
    vess_flag = CharField()
    vess_owner = CharField()
    remarks = CharField()


class ConsolidatedAddress(BaseModel):
    ent_num = IntegerField()
    add_num = IntegerField()
    address = CharField()
    city_state_province_postal  = CharField()
    country = CharField()
    add_remarks = CharField()

class ConsolidatedAlternateIdentity(BaseModel):
    ent_num = IntegerField()
    alt_num = IntegerField()
    alt_type = CharField()
    alt_name = CharField()
    alt_remarks = CharField()

class ConsolidatedComment(BaseModel):
    ent_num = IntegerField()
    comments = CharField()

def populate_db(filename, dbclass, keys):
    with open(filename, 'r') as fh:
        reader = csv.reader(fh, delimiter=',')

        for row in reader:
            newrow = []
            for ii in row:
                if ii == "-0- ":
                    newrow.append("")
                else:
                    newrow.append(ii)

            vals = dict(zip(keys,newrow))
            try:
                new_entry = dbclass.create(**vals)
                print(new_entry)
                new_entry.save()
            except ValueError:
                print("The data from this row are corrupted.")


if __name__ == "__main__":

    tables = [SDN, SDNAddress, SDNAlternateIdentity, SDNComment, Consolidated, ConsolidatedAddress, ConsolidatedAlternateIdentity, ConsolidatedComment]
    for tt in tables:
        try:
            tt.delete()
        except OperationalError:
            print("Table doesn't exist")
    


    # Populate the db 
    database.connect()
    database.create_tables(tables)

    #  SDN file
    sdn_file = "sdn/SDN.CSV"
    sdn_keys = ["ent_num", "sdn_name", "sdn_type", "program", "title", "call_sign", "vess_type", "tonnage", "grt", "vess_flag","vess_owner", "remarks"]
    populate_db(sdn_file, SDN, sdn_keys)

    #  ADD file
    add_file = "sdn/ADD.CSV"
    add_keys = ["ent_num","add_num","address","city_state_province_postal","country","add_remarks"]
    populate_db(add_file, SDNAddress, add_keys)

    # ALT file
    alt_file = "sdn/ALT.CSV"
    alt_keys = ["ent_num","alt_num","alt_type","alt_name","alt_remarks"]
    populate_db(alt_file, SDNAlternateIdentity, alt_keys)

    # Comments file
    comments_file = "sdn/sdn_comments.csv"
    comments_keys = ["ent_num","comments"]
    populate_db(comments_file, SDNComment, comments_keys)


    ### Consolidated Files

    # Consolidated file
    cons_file = "consolidated/cons_prim.csv"
    cons_keys = ["ent_num", "sdn_name", "sdn_type", "program", "title", "call_sign", "vess_type", "tonnage", "grt", "vess_flag","vess_owner", "remarks"]
    populate_db(cons_file, Consolidated, cons_keys)

    # Consolidated Alt file
    cons_alt_file = "consolidated/cons_alt.csv"
    cons_alt_keys = ["ent_num","alt_num","alt_type","alt_name","alt_remarks"]
    populate_db(cons_alt_file, ConsolidatedAlternateIdentity, cons_alt_keys)


    # Consolidated Address file
    cons_add_file = "consolidated/cons_add.csv"
    cons_add_keys = ["ent_num","add_num","address","city_state_province_postal","country","add_remarks"]
    populate_db(cons_add_file, ConsolidatedAddress, cons_add_keys)


    # Consolidated Comments file
    cons_comments_file = "consolidated/cons_comments.csv"
    cons_comments_keys = ["ent_num","comments"]
    populate_db(cons_comments_file, ConsolidatedComment, cons_comments_keys)





    # Now add all of these to the running elasticsearch instance
    def add_to_es(dbclass):
        name = dbclass.__name__
        print("The name of this class is {}".format(name))
        res = dbclass.select()
        for ii in res:
            result = es.index(index="ofac", doc_type="SDN", body=model_to_dict(ii))

    es = Elasticsearch()

    es.indices.delete(index=["ofac"])
    tables = [SDN, SDNAddress, SDNAlternateIdentity, SDNComment, Consolidated, ConsolidatedAddress, ConsolidatedAlternateIdentity, ConsolidatedComment]
    for ii in tables:
        add_to_es(ii)





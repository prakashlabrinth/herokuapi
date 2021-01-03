import psycopg2
from flask import Flask, request, views
from flask_restful import Api
from sqlalchemy import Column, String, Integer,Date,BOOLEAN
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import os

app = Flask(__name__)
api = Api(app)

Base  = declarative_base()
database_url= "postgres://zfxzhztsbsrduy:1527523af8a571421e576d53365e5d836069210d9148e37ce93280145c6a4b65@ec2-3-215-40-176.compute-1.amazonaws.com:5432/dru2o63lvhorn"

#disable sqlalchemy pool using nullpool as by default postgress has its own pool
engine = create_engine(database_url,echo=True, poolclass=NullPool)

conn = engine.connect()

Session = sessionmaker(bind=engine)
session = Session()
print("Session ---> {}".format(session))

class ProductEnquiryForms(Base):
    __tablename__ = 'productenquiryforms'
    CustomerName =Column("customername",String)
    Gender =Column("gender",String)
    Age =Column("age",Integer)
    Occupation =Column("occupation",String)
    MobileNo =Column("mobileno",Integer,primary_key=True)
    Email =Column("email",String)
    VechicleModel =Column("vechiclemodel",String)
    State =Column("state",String)
    District =Column("district",String)
    City =Column("city",String)
    ExistingVehicle =Column("existingvehicle",String)
    DealerState =Column("dealerstate",String)
    DealerTown =Column("dealertown",String)
    Dealer =Column("dealer", String)
    BriefAboutEnquiry =Column("briefaboutenquiry", String)
    ExpectedDateofPurchase =Column("expecteddateofpurchase", Date)
    IntendedUsage =Column("intendedusage", String)
    Senttodealer = Column("senttodealer", BOOLEAN)
    DealerCode = Column("dealercode",String)
    Comment = Column("comment", String)
    Createddate = Column("createddate", Date)
    isPurchased = Column("ispurchased", BOOLEAN)
    ProductsPurchasedCount = Column("productspurchasedcount", String)

class EmployeeDetails(Base):
    __tablename__ = 'employee'
    Name =Column("name",String)
    Age =Column("age",Integer)
    DOB = Column("dob", Date)
    Salary = Column("salary", String)
    BankName = Column("bankname", String)
    BankAccountNumber = Column("bankaccountnumber", Integer)
    PanCard = Column("pancard", String)
    IFSCcode = Column("ifsccode", String)
    Department = Column("department", String)
    SubDepartment = Column("subdepartment", String)
    EmployeeId = Column("employeeid", String, primary_key=True)

class PersonalInfo(Base):
    __tablename__ = 'personalinfo'
    Name =Column("name",String)
    Age =Column("age",Integer)
    DOB = Column("dob", Date)
    EmployeeId = Column("employeeid", String, primary_key=True)

class FinancialInfo(Base):
    __tablename__ = 'financialinfo'
    Salary = Column("salary", String)
    BankName = Column("bankname", String)
    BankAccountNumber = Column("bankaccountnumber", Integer)
    PanCard = Column("pancard", String)
    IFSCcode = Column("ifsccode", String)
    EmployeeId = Column("employeeid", String, primary_key=True)

class Department(Base):
    __tablename__ = 'department'
    Department = Column("department", String)
    SubDepartment = Column("subdepartment", String)
    EmployeeId = Column("employeeid", String, primary_key=True)

@app.route('/viewall', methods=['GET'])
def viewall():
    #dealercode = request.args.get("dealercode")
    result = session.query(EmployeeDetails).all()
    result = [item.__dict__ for item in result]
    print("Result data {}".format(result))
    return str(result)

@app.route('/employeedetails', methods=['GET'])
def view():
    Em_ID = request.args.get("employeeid")
    result = session.query(EmployeeDetails).filter(EmployeeDetails.EmployeeId==Em_ID).all()
    result = [item.__dict__ for item in result]
    print("Result data {}".format(result))
    for item in result:
        item.pop('_sa_instance_state')
    return str(result)

@app.route('/postpersonalinfo', methods=['POST'])
def postpersonalinfo():
    request_body = request.get_json(force=True)
    for index,item in enumerate(request_body):
        record = PersonalInfo(Name = item["name"],
                                     Age = item["age"],
                                     DOB = item["dob"],
                                     EmployeeId =item["employeeid"])
        session.add_all([record])
    session.commit()
    return ("data inserted in PersonalInfo table successfully")

@app.route('/postfinancialinfo', methods=['POST'])
def postfinancialinfo():
    request_body = request.get_json(force=True)
    for index,item in enumerate(request_body):
        record = FinancialInfo(
                                     Salary = item["salary"],
                                     BankName = item["bankname"],
                                     BankAccountNumber =item["bankaccountnumber"],
                                     PanCard =item["pancard"],
                                     IFSCcode = item["ifsccode"],
                                     EmployeeId =item["employeeid"])
        session.add_all([record])
    session.commit()
    return ("data inserted in FinancialInfo table successfully")

@app.route('/postdepartment', methods=['POST'])
def postdepartment():
    request_body = request.get_json(force=True)
    for index,item in enumerate(request_body):
        record = Department(
                                     Department = item["department"],
                                     SubDepartment = item["subdepartment"],
                                     EmployeeId =item["employeeid"])
        session.add_all([record])
    session.commit()
    return ("data inserted in Department table successfully")

@app.route('/putemprecord', methods=['PUT'])
def putemprecord():
     Emp_Id = request.args.get("employeeid")
     request_body = request.get_json(force=True)
     try:
         result = session.query(Department).filter(Department.EmployeeId==Emp_Id)\
            .update({Department.Department: request_body[0]["department"]})
         session.commit()
         return str(result)
     finally:
         session.close()

@app.route('/delemprecord',methods = ['DELETE'])
def delrecodelemprecordrd():
    from flask import request
    print("parameter is {}".format(request.args))
    Emp_ID = request.args.get("employeeid")
    try:
        result = session.query(Department).filter(Department.EmployeeId == Emp_ID).delete()
        session.commit()
        return str(result)
    finally:
        pass

if __name__ == "__main__":
    app.run(debug=True)
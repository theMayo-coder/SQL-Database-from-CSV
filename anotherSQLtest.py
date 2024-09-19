import csv
import zipfile36 as zipfile
from typing import Optional, List
from sqlmodel import Field, SQLModel, create_engine, Session, select, Relationship

file = ''

with zipfile.ZipFile(r"C:\Users\mrdud\OneDrive\readCSV\organizations-1000.zip") as zipped:
    with zipped.open("organizations-1000.csv") as csv:
        print(csv.readline())
        file = str(csv.read(), 'UTF-8')

class Org_Industry_Link(SQLModel, table=True):
    org_id: Optional[int] = Field(default=None, foreign_key="organization.id", primary_key=True)
    industry_id: Optional[int] = Field(default=None, foreign_key="industry.id", primary_key=True)

class Organization(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    organization_id: str
    website: str
    description: str
    founded: int
    num_employees: int
    country_id: Optional[int] = Field(default=None, foreign_key="country.id")
    country: "Country" = Relationship(back_populates="organizations")
    industries: List["Industry"] = Relationship(back_populates="organizations", link_model=Org_Industry_Link)

class Industry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    industry: str
    organizations: List["Organization"] = Relationship(back_populates="industries", link_model=Org_Industry_Link)

class Country(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    country: str
    organizations: List["Organization"] = Relationship(back_populates="country")

sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

engine = create_engine(sqlite_url, echo=False)

def gen_rows():
    rows = file.split('\n')
    for row in range(1000):
        elements = rows[row].split(',')
        if elements[2][0] == '"':
            elements[2] = (elements[2]+','+elements[3]).replace('"', '')
            del elements[3]
        yield elements

def make_organizations():
    with Session(engine) as session:

        industries = []
        countries = []
        for elements in gen_rows():

            new_c = None
            new_i = []
            new_industries = []

            if elements[4] not in countries:
                new_country = Country(country = elements[4])
                countries.append(elements[4])
                session.add(new_country)

            new_c = session.exec(select(Country).where(Country.country==elements[4])).one()

            for ind in elements[7].split(' / '):
                if ind not in industries:
                    new_industry = Industry(industry=ind)
                    industries.append(ind)
                    session.add(new_industry)

                new_i = session.exec(select(Industry).where(Industry.industry==ind)).one()
                new_industries.append(new_i)

            new_org = Organization(
                organization_id=elements[1], name=elements[2], website=elements[3], description=elements[5], founded=elements[6],
                num_employees=elements[8], country=new_c, industries=new_industries
            )
            
            session.add(new_org)

        session.commit()


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def print_organizations():
    with Session(engine) as session:
        statement = select(Organization)
        organizations = session.exec(statement)
        for o in organizations:
            print(o, end=" industries=")
            print(o.industries)
            print()
        

def main():
    #create_db_and_tables()
    #make_organizations()
    print_organizations()

if __name__ == "__main__":
    main()






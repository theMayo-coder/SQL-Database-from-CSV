import csv
import io
import zipfile36 as zipfile
from typing import Optional, List
from sqlmodel import Field, SQLModel, create_engine, Session, select, Relationship

#junction table creating many-to-many relationship between orgs and industries
class Org_Industry_Link(SQLModel, table=True):
    org_id: Optional[int] = Field(default=None, foreign_key="organization.id", primary_key=True)
    industry_id: Optional[int] = Field(default=None, foreign_key="industry.id", primary_key=True)

#organization table
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

#industry table
class Industry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    industry: str
    organizations: List["Organization"] = Relationship(back_populates="industries", link_model=Org_Industry_Link)

#country table
class Country(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    country: str
    organizations: List["Organization"] = Relationship(back_populates="country")

#create database file and engine
sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"
engine = create_engine(sqlite_url, echo=False)

#generate and return individual rows to add to organizations table
def gen_rows():
    #open zipped file and read organizations csv
    file = ''
    with zipfile.ZipFile(r"C:\Users\mrdud\OneDrive\readCSV\organizations-1000.zip") as zipped:
        with zipped.open("organizations-1000.csv") as csvfile:

            #make binary file into text
            text_file = io.TextIOWrapper(csvfile, encoding='utf-8')
            reader = csv.reader(text_file)

            #skip first line with headers
            next(reader)

            #go through 1000 organizations and yield column data
            for i in range(1000):
                yield next(reader)

def make_organizations():
    with Session(engine) as session:

        industries = []
        countries = []

        #get organization information one at a time (row by row)
        for elements in gen_rows():

            new_c = None
            new_i = None
            new_industries = []

            #add new country to countries table
            if elements[4] not in countries:
                new_country = Country(country = elements[4])
                countries.append(elements[4])
                session.add(new_country)
            new_c = session.exec(select(Country).where(Country.country==elements[4])).one()

            for ind in elements[7].split(' / '):
                #add new industry to industries table
                if ind not in industries:
                    new_industry = Industry(industry=ind)
                    industries.append(ind)
                    session.add(new_industry)

                #select industry if not 
                new_i = session.exec(select(Industry).where(Industry.industry==ind)).one()
                new_industries.append(new_i)

            #create and add new organization to session
            new_org = Organization(
                organization_id=elements[1], name=elements[2], website=elements[3], description=elements[5], founded=elements[6],
                num_employees=elements[8], country=new_c, industries=new_industries
            )
            session.add(new_org)

        #commit changes
        session.commit()


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

#print all organizations
def print_organizations():
    with Session(engine) as session:
        statement = select(Organization)
        organizations = session.exec(statement)
        #print all organizations
        for o in organizations:
            print(o, end=" industries=")
            print(o.industries)
            print()

#get names of orgs in a specific country
def orgs_in_country(c):
    with Session(engine) as session:
        statement = select(Country).where(Country.country == c)
        country = session.exec(statement).one()

        for org in country.organizations:
            print(org.name)
        

def main():
    #only create db, tables, and organizations once 
    #create_db_and_tables()
    #make_organizations()

    print_organizations()
    orgs_in_country("Israel")


if __name__ == "__main__":
    main()






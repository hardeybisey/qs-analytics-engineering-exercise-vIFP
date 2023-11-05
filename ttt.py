from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, UniqueConstraint, DECIMAL, Session
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Bar(Base):
    __tablename__ = 'bars'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    UniqueConstraint('name')

class BarStock(Base):
    __tablename__ = 'bar_stock'
    id = Column(Integer, primary_key=True)
    bar_id = Column(Integer, ForeignKey('bars.id'))
    glass_id = Column(Integer, ForeignKey('glasses.id'))
    stock = Column(Integer)
    
class Transaction(Base):
    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True)
    drink_id = Column(Integer, ForeignKey('cocktail.id'))
    bar_id = Column(Integer, ForeignKey('bars.id'))
    date = Column(DateTime, nullable=False)
    amount = Column(DECIMAL(2), nullable=False)
    
class Glass(Base):
    __tablename__ = 'glasses'
    id = Column(Integer, primary_key=True)
    glass = Column(String)
    UniqueConstraint('glass')

class Cocktail(Base):
    __tablename__ = 'cocktail'
    id = Column(Integer, primary_key=True)
    glass_id = Column(Integer, ForeignKey('glasses.id'))
    cocktail = Column(String, nullable=False)
    UniqueConstraint('cocktail', 'glass_id')
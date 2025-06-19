from abc import ABC, abstractmethod

class Bank(ABC):
    bank_name = 'Nabil Bank'

    def __init__(self, name, pin, address, balance=0,status ='Active'):
        self.id = id(self)
        self.name = name
        self.__pin = pin
        self.address = address
        self.balance = balance
        self.status = status

    @abstractmethod
    def deposite(self, amount):
        pass

    @abstractmethod
    def withdrawal(self,amount):
        pass

    @abstractmethod
    def transfer(self,amount,receiver):
        pass

    def check_data_type(data_type):
        def check_valid(func):
            def wrapper(*args):
                if not isinstance (args[1],data_type):
                    print('Invalid DataType')
                    return False
                return func(*args)
            return wrapper
        return check_valid
     
    def check_validation(func):
        def wrapper(self,amount,*args):
            if self.balance < amount:
                print(f"Your balance is insufficient {self.balance}" )
                return False
            return func(self,amount,*args)
        return wrapper
       
class Staff(Bank):
    def __init__(self, name, pin, address, balance=0, status='Active'):
        super().__init__(name, pin, address, balance, status)
        self.break_time = ['12:00 pm' , '[1:00 pm]']
        print(f"{self.name} is an employee based on {self.address} branch and her scheduled break time is {self.break_time}")

    def deposite(self, amount):
        if amount < 0:
            print(f"Amount can't be negative {self.amount}")
            return False
        self.balance += amount
        print(f"Your balance is {self.balance}")

    @Bank.check_data_type(int)
    @Bank.check_validation
    def withdrawal(self,amount):
        print(f"{self.name} your {amount} has been withdrawn")
        self.balance -= amount
        print(f"Your balance is {self.balance}")
      
    @Bank.check_data_type(int)
    @Bank.check_validation  
    def transfer(self, amount, receiver):
        print(f"{self.name} has transfer amount {amount} to {receiver.name}")
        self.balance -= amount
        receiver.balance += amount
        print(f"Your balance is {self.balance}")
        print(f"{receiver.name} balance is {receiver.balance}")

    @property
    def get_pin(self):
        return self.__pin

    @get_pin.setter
    def pin(self, new_pin):
        self.__pin = new_pin
    print("Pin is updated.")

class Customer(Bank):
    def __init__(self, name, pin, address, balance=0, status='Active'):
        super().__init__(name, pin, address, balance, status)

    def deposite(self, amount):
        if amount < 0:
            print(f"Amount can't be negative {self.amount}")
            return False
        self.balance += amount
        print(f"Your balance is {self.balance}")
    
    @Bank.check_data_type(int)
    @Bank.check_validation
    def withdrawal(self,amount):
        print(f"{self.name} your {amount} has been withdrawn")
        self.balance -= amount
        print(f"Your balance is {self.balance}")

    @Bank.check_data_type(int)
    @Bank.check_validation  
    def transfer(self, amount, receiver):
        print(f"{self.name} has transfer amount {amount} to {receiver.name}")
        self.balance -= amount
        receiver.balance += amount
        print(f"Your balance is {self.balance}")

    @property
    def get_pin(self):
        return self.__pin

    @get_pin.setter
    def pin(self, new_pin):
        self.__pin = new_pin
    print("Pin is updated.")



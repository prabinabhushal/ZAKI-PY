from abc import abstractmethod,ABC

class Employee(ABC):
    def __init__(self, name: str, age: int, address: str):
        self.name = name
        self.__age = age
        self.address = address

    @abstractmethod
    def data(self):                        
        pass

class Intern(Employee):
    count = 0

    def __init__(self, name, age, address,salary = 0):
        super().__init__(name, age, address)
        self.salary = salary                
        self.team = ['data', 'developer']  
        self.courses = {'sql', 'python'}     
        self.marks = {'bachelor': 80}       
        self.active = True               
        Intern.count += 1

    def data(self):                        
        print(f"{self.name} is a new intern from {self.address} with salary {self.salary}")

    def check_active(self):
        if self.active: 
            print(f"{self.name} is active.")
        else:
            print(f"{self.name} is not active.")

    def performance_level(self):
        if self.salary >= 15000: 
            print(f"{self.name} is a High Performer.")
        else:
            print(f"{self.name} is a Regular Performer.")

    def add_team(self, new_team):
        self.team.append(new_team)
        print(f"All teams : {self.team}")

    def add_courses(self, new_courses):
        self.courses.add(new_courses)
        print(f"All courses : {self.courses}")

    def add_marks(self, level, marks):
        self.marks[level] = marks
        print(f"All marks : {list(self.marks.values())}")

    def check_salary(self):
        if self.salary < 0:
            raise ValueError("Salary cannot be negative")

def all_interns(intern_list):
    for intern in intern_list:
        intern.data()

new = Intern('Prabina', 23, 'Koteshwor')
new1 = Intern('Namuna', 23, 'Kalanki')

new.check_salary()
all_interns([new, new1])
new.add_team('QA')
new.add_courses('mrf')
new.add_marks('masters', 90)
print("Total Interns:", Intern.count)

class Mentor(Intern):  
    def __init__(self, name, age, address, salary, mentor):
        super().__init__(name, age, address, salary)
        self.mentor = mentor

    def data(self):  
        super().data()  
        print(f"Mentored by: {self.mentor}")

mentor1 = Mentor('Prabina', 23, 'Koteshwor', 20000, "ram")
mentor1.data()

print(new1.age)



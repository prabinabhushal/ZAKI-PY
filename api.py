# Question:
# api url = "https://api.le-systeme-solaire.net/rest/bodies/"
# For all heavenly bodies check if they are parameter with key 'isPlanet'
# If it is planet
# get the name of planet with key 'englishName'
# get the semi_major_axis (initially in km) using key 'semimajorAxis'
# get the distance using formula semi_major_axis/1000000 (convert km to million km)
# this distance is the distance from sun
# populate Parent class that will have two attributes: name, distance
# Using insertion sort, sort the planets by distance from the sun, display it in ascending or descending order depending on user input
# user input must be either apiA or apiD - api denotes that the api related code must be executed and the ending A or D denotes the sorting order

import requests

class Parent:
    def __init__(self, name, distance_km):
        self.name = name
        self.distance = distance_km

    def __str__(self):
        return f"Planet :{self.name}, Distance: {self.distance} million km"
def planets():
    api_url = "https://api.le-systeme-solaire.net/rest/bodies/"
    response = requests.get(api_url)
    data = response.json()

    new_bodies = []

    for body in data['bodies']:
        if body.get('isPlanet'):
            name = body.get('englishName')
            semi_major_axis_km = body.get('semimajorAxis')
            if semi_major_axis_km is not None:
                distance_km = semi_major_axis_km / 1000000
                planet = Parent(name, distance_km)
                new_bodies.append(planet)

    return new_bodies

def insertion_sort(planets, ascending=True):
    for i in range(1, len(planets)):
        key = planets[i]
        j = i - 1
        while j >= 0 and ((planets[j].distance > key.distance) if ascending else (planets[j].distance < key.distance)):
            planets[j + 1] = planets[j]
            j -= 1
        planets[j + 1] = key
    return planets

def show_ascending(logger):
    planet_new = planets()
    sorted_planets = insertion_sort(planet_new, ascending=True)
    logger.info("Planets sorted in ascending order by distance:")
    for planet in sorted_planets:
        logger.info(planet)
      

def show_descending(logger):
    planet_new = planets()
    sorted_planets = insertion_sort(planet_new, ascending=False)
    logger.info("Planets sorted in descending order by distance:")
    for planet in sorted_planets:
        logger.info(planet)
     


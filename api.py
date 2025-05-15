# # Question:
# # api url = "https://api.le-systeme-solaire.net/rest/bodies/"
# # For all heavenly bodies check if they are parameter with key 'isPlanet'
# # If it is planet
# # get the name of planet with key 'englishName'
# # get the semi_major_axis (initially in km) using key 'semimajorAxis'
# # get the distance using formula semi_major_axis/1000000 (convert km to million km)
# # this distance is the distance from sun
# # populate Parent class that will have two attributes: name, distance
# # Using insertion sort, sort the planets by distance from the sun, display it in ascending or descending order depending on user input
# # user input must be either apiA or apiD - api denotes that the api related code must be executed and the ending A or D denotes the sorting order


import requests
import argparse

class Parent:
    def __init__(self, name, distance_km):
        self.name = name
        self.distance = distance_km  # in million km

    def __str__(self):
        return f"Planet: {self.name}, Distance: {self.distance:.2f} million km"

def new_planets():
    api_url = "https://api.le-systeme-solaire.net/rest/bodies/"
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()

    planets = []
    for body in data['bodies']:
        if body.get('isPlanet') and body.get('semimajorAxis'):
            name = body['englishName']
            distance_million_km = body['semimajorAxis'] / 1000000
            planets.append(Parent(name, distance_million_km))
    return planets

def insertion_sort(planets, ascending=True):
    for i in range(1, len(planets)):
        p = planets[i]
        j = i - 1
        while j >= 0 and (
            (planets[j].distance > p.distance) if ascending else (planets[j].distance < p.distance)
        ):
            planets[j + 1] = planets[j]
            j -= 1
        planets[j + 1] = p
    return planets

def bubble_sort(planets, ascending=True):
    n = len(planets)
    for i in range(n):
        for j in range(0, n - i - 1):
            if (planets[j].distance > planets[j + 1].distance) if ascending else (planets[j].distance < planets[j + 1].distance):
                planets[j], planets[j + 1] = planets[j + 1], planets[j]
    return planets

def main():
    parser = argparse.ArgumentParser(description="Sort planets by distance from the sun.")
    parser.add_argument("--order", choices=["apiA", "apiD"], help="apiA = Ascending | apiD = Descending")
    parser.add_argument("--sort", choices=["bubble", "insertion"], help="Sorting method: bubble or insertion")

    args = parser.parse_args()

    # Ask user if arguments are missing
    if not args.order:
        args.order = input("Enter sorting order (apiA for ascending, apiD for descending): ").strip()
        if args.order not in ["apiA", "apiD"]:
            print("Invalid order input. Exiting.")
            return

    if not args.sort:
        args.sort = input("Enter sorting method (bubble or insertion): ").strip()
        if args.sort not in ["bubble", "insertion"]:
            print("Invalid sort method. Exiting.")
            return

    ascending = args.order == "apiA"
    planets = new_planets()

    if args.sort == "bubble":
        sorted_planets = bubble_sort(planets, ascending)
    else:
        sorted_planets = insertion_sort(planets, ascending)

    direction = "Ascending" if ascending else "Descending"
    print(f"\nPlanets sorted in {direction} order using {args.sort} sort:\n")
    for planet in sorted_planets:
        print(planet)

if __name__ == "__main__":
    main()

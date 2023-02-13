from model.resources.ResourceMapper import ResourceMapper

mapper = ResourceMapper()
dictionary = mapper.get_resource_dict()
print(dictionary)

for key in dictionary:
    print(key)
    print(dictionary[key])

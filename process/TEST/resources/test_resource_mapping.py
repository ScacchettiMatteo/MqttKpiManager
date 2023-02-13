from model.resources.ResourceMapper import ResourceMapper

mapper = ResourceMapper()
dictionary = mapper.get_resource_dict()
print(dictionary)

for key in dictionary:
    print(key)
    dictionary[key].set_value(1)
    print(dictionary[key])

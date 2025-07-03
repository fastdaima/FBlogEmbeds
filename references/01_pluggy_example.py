import pluggy 

hookspec = pluggy.HookspecMarker('project1')
hookimpl = pluggy.HookimplMarker('project1')

class MySpec: 
    @hookspec 
    def myhook(self, arg1, arg2):
        pass 


class Plugin1: 

    @hookimpl
    def myhook(self, arg1, arg2):
        print(f"Plugin1: {arg1}, {arg2}")
        return arg1 + arg2 
    

class Plugin2: 
    @hookimpl
    def myhook(self, arg1, arg2):
        print(f"Plugin2: {arg1}, {arg2}")
        return arg1 * arg2
    

pm = pluggy.PluginManager('project1')
pm.add_hookspecs(MySpec)

pm.register(Plugin1())
pm.register(Plugin2())

results = pm.hook.myhook(arg1=2, arg2=3)
print("Results:", results)


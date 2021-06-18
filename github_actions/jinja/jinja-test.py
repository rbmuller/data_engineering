import jinja2

string_template = """
Hi {{name}} this is a test'
"""
template = jinja2.Template(string_template)
rendered_template = template.render({"name":"Robson"})

print(rendered_template)

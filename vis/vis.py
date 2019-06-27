from IPython.display import IFrame
import json
import uuid

def vis_network(nodes, edges, physics=False, height="400", filename=None, node_shape='dot', edge_width=1.0):
    # slightly adapted version compared to the original by Nicole White
    # https://github.com/nicolewhite/neo4j-jupyter/blob/master/scripts/vis.py
    # height parameter added to allow for larger IFrames
    # filename parameter added to determine file location
    # added check that filename ends with .html
    
    html = """
    <html>
    <head>
      <script type="text/javascript" src="../../third_party/vis_original/dist/vis.js"></script>
      <link href="../../third_party/vis_original/dist/vis.css" rel="stylesheet" type="text/css">
    </head>
    <body>

    <div id="{id}"></div>

    <script type="text/javascript">
      var nodes = {nodes};
      var edges = {edges};

      var container = document.getElementById("{id}");

      var data = {{
        nodes: nodes,
        edges: edges
      }};

      var options = {{
          nodes: {{
              shape: {node_shape},
              size: 30,
              font: {{
                  size: 16
              }}
          }},
          edges: {{
              font: {{
                  size: 14,
                  align: 'middle'
              }},
              color: 'gray',
              width: {edge_width},
              arrows: {{
                  to: {{enabled: true, scaleFactor: 0.8}}
              }},
              smooth: {{enabled: false}}
          }},
          physics: {{
              enabled: {physics},
              barnesHut: {{
                  gravitationalConstant: -4000,
                  centralGravity: 0.1,
                  springLength: 170,
                  springConstant: 0.04,
                  damping: 0.09,
                  avoidOverlap: 0.5
              }},
              forceAtlas2Based: {{
                  gravitationalConstant: -50,
                  centralGravity: 0.01,
                  springConstant: 0.08,
                  springLength: 170,
                  damping: 0.4,
                  avoidOverlap: 0
              }},
              repulsion: {{
                  centralGravity: 0.1,
                  springLength: 170,
                  springConstant: 0.04,
                  nodeDistance: 100,
                  damping: 0.09
              }},
              hierarchicalRepulsion: {{
                  centralGravity: 0.0,
                  springLength: 170,
                  springConstant: 0.01,
                  nodeDistance: 120,
                  damping: 0.09
              }},
              solver: 'barnesHut'
          }}
      }};

      var network = new vis.Network(container, data, options);

    </script>
    </body>
    </html>
    """

    unique_id = str(uuid.uuid4())
    html = html.format(id=unique_id, nodes=json.dumps(nodes), edges=json.dumps(edges),
                       physics=json.dumps(physics), node_shape=json.dumps(node_shape), edge_width=json.dumps(edge_width))

    if filename is None:
        filename = "graph-{}.html".format(unique_id)
    elif not filename.endswith(".html"):
        filename = filename + ".html"
    
    filename = "pics/vis_generated/" + filename
    
    file = open(filename, "w")
    file.write(html)
    file.close()

    return IFrame(filename, width="100%", height=height)

def draw(graph, options, physics=False, limit=100):
    # The options argument should be a dictionary of node labels and property keys; it determines which property
    # is displayed for the node label. For example, in the movie graph, options = {"Movie": "title", "Person": "name"}.
    # Omitting a node label from the options dict will leave the node unlabeled in the visualization.
    # Setting physics = True makes the nodes bounce around when you touch them!
    query = """
    MATCH (n)
    WITH n, rand() AS random
    ORDER BY random
    LIMIT {limit}
    OPTIONAL MATCH (n)-[r]->(m)
    RETURN n AS source_node,
           id(n) AS source_id,
           r,
           m AS target_node,
           id(m) AS target_id
    """

    data = graph.run(query, limit=limit)

    nodes = []
    edges = []

    def get_vis_info(node, id):
        node_label = list(node.labels())[0]
        prop_key = options.get(node_label)
        vis_label = node.properties.get(prop_key, "")

        return {"id": id, "label": vis_label, "group": node_label, "title": repr(node.properties)}

    for row in data:
        source_node = row[0]
        source_id = row[1]
        rel = row[2]
        target_node = row[3]
        target_id = row[4]

        source_info = get_vis_info(source_node, source_id)

        if source_info not in nodes:
            nodes.append(source_info)

        if rel is not None:
            target_info = get_vis_info(target_node, target_id)

            if target_info not in nodes:
                nodes.append(target_info)

            edges.append({"from": source_info["id"], "to": target_info["id"], "label": rel.type()})

    return vis_network(nodes, edges, physics=physics)

def drawSubgraph(subgraph, options, physics=False, height="400", filename=None, node_shape='dot', edge_width=1.0):
    # slightly adapted version of draw which fixes compatibility issues with py2neo
    # draws a py2neo Subgraph object in an IFrame which can be displayed in a jupyter notebook
    # rendering is based on the vis.js library
    
    nodes = []
    edges = []
    
    def get_vis_info(node):
        node_label = list(node.labels)[0]
        prop_key = options.get(node_label)
        vis_label = node[prop_key]
        #return {"id": node["node_id"], "label": vis_label, "group": node_label, "title": repr(dict(node))}
        return {"id": node["node_id"], "label": vis_label, "group": node_label, "title": vis_label}
    
    for node in subgraph.nodes:
        info = get_vis_info(node)
        if info not in nodes:
            nodes.append(info)
    from py2neo.data import walk
    for rel in subgraph.relationships:
        elements = list(walk(rel))
        source_info = get_vis_info(elements[0])
        target_info = get_vis_info(elements[2])
        edges.append({"from": source_info["id"], "to": target_info["id"], "label": elements[1].__class__.__name__})
        
    return vis_network(nodes, edges, physics=physics, height=height, filename=filename, node_shape=node_shape, edge_width=edge_width)

def print_set_postgres(cursor):
    # print result set in the same format as in the Relational Algebra notebook
    # implementation based on print_set in relation.py
    column_names = [col.name for col in cursor.description]
    tuples = cursor.fetchall()
    print_set(column_names, tuples)

def print_set_neo4j(cursor):
    # print result set in the same format as in the Relational Algebra notebook
    # implementation based on print_set in relation.py
    column_names = cursor.keys()
    tuples = cursor.to_ndarray()
    print_set(column_names, tuples)

def print_set(column_names, tuples):
    # compare to implementation of __str__ in relation.py
    rel = '[{}]'.format("Result")
    attrs = ','.join([' {}'.format(column_names[i]) for i in range(len(column_names))])
    
    # compare to implementation of print_set in relation.py
    target = '{} : {{[{} ]}}'.format(rel, attrs) + '\n{\n'
    for tup in tuples:
        target += '\t(' + ', '.join(str(attr) for attr in tup) + '),\n'
    target = target.rstrip("\n").rstrip(",")
    target += '\n}'
    print(target)
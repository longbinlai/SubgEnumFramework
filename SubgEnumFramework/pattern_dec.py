import networkx as nx
import itertools as it
import sys
import os
import math

# To run the pattern decomposition, please download graphgen from:
# https://www-complexnetworks.lip6.fr/~latapy/FV/generation.html
# It will generate the degree sequence of a random graph with power-law
# distribution. We should configure the following gen_graph_dir if needed.
class PatternDec:
    ''' Degree sequence'''
    deg_array = []
    ''' Sum of degree to the power n '''
    sum_of_power = {}
    ''' Number of vertices in the data graph'''
    num_of_data_vertices = 0
    ''' Number of edges in the data graph'''
    num_of_data_edges = 0
    ''' The average degree '''
    avg_deg = 0
    ''' Power-law Exponent'''
    power_law_exp = 2.1
    ''' Pattern Graph objects'''
    pattern_graph = None
    ''' Subgraphs Hash Table'''
    subgraph_hash_table = {}
    ''' unit communication cost '''
    unit_cost_com = 0
    ''' unit local mem access cost'''
    unit_cost_local_mem = 0
    ''' unit global mem access cost'''
    unit_cost_global_mem = 0
    ''' Buffer size in pages for external join'''
    buffer_size = 4096
    ''' Merge order for the external sort'''
    merge_order = 10
    ''' Page size'''
    page_size = 4096
    '''The rootdir of the gengraph directory'''
    gen_graph_dir = 'gengraph'

    def   __init__(self, _num_vertices, _avg_deg = 10,  _power_law_exp = 2.1,  _c_com = 3, _c_lm = 1,  _c_gm = 2,  _bs = 1024,  _order = 10,  _ps = 4096):
        self.num_of_data_vertices = _num_vertices
        self.avg_deg = _avg_deg
        self.num_of_data_edges = int(_num_vertices * _avg_deg / 2)
        self.power_law_exp = _power_law_exp
        self.unit_cost_com = _c_com
        self.unit_cost_local_mem = _c_lm
        self.unit_cost_global_mem = _c_gm
        self.buffer_size = _bs
        self.merge_order = _order
        
    def __del__(self):
        self.pattern_graph.clear()
        self.subgraph_hash_table.clear()
    
    def read_pattern_from_file(self, _pattern_file = '',  _delim = '\t'):
        f = open(_pattern_file,  'r')
        g = nx.Graph()
        for line in f.readlines():
            node1,  node2 = line.strip().split(_delim)
            g.add_edge(node1,  node2)
        self.pattern_graph = g
    
    def set_pattern_graph(self,  _g):
        self.pattern_graph = _g
        
    def gen_deg_seq(self):
        min_deg = 1
        max_deg = int(math.sqrt(self.num_of_data_vertices * self.avg_deg))
        cmd_deg = self.gen_graph_dir + "/bin/distrib " + str(self.num_of_data_vertices) + " " + str(self.power_law_exp) + " " + str(min_deg) + " " + \
                str(max_deg) + " " + str(self.avg_deg)  + " > tmp"
        print(cmd_deg)
        os.system(cmd_deg)

        f = open('tmp',  'r')
        for line in f.readlines():
            deg,  num = line.split()
            for i in range(0,  int(num)):
                self.deg_array.append(int(deg))
        f.close()
    
    ''' We precompute the sum of the degree under order 1 ... upper_exp'''
    def cal_sum_of_power(self,  upper_exp = 5):
        for i in range(1, upper_exp):
            s = 0.0
            for deg in self.deg_array:
                s += (self.pow(deg,  i))
            if(not self.sum_of_power.has_key(i)):
                self.sum_of_power[i] = s
    
    ''' Calculate gamma for the first case'''
    def cal_gamma1(self,  _d):  
        sum1 = 0.0
        sum2 = 0.0
        if(not self.sum_of_power.has_key(_d)):
            for deg in self.deg_array:
                sum1 += (self.pow(deg,  _d))
        else:
            sum1 = self.sum_of_power[_d]
            
        if(not self.sum_of_power.has_key(_d + 1)):
            for deg in self.deg_array:
                sum2 += (self.pow(deg, _d + 1))
        else:
            sum2 = self.sum_of_power[_d + 1]
        ratio = sum2 / sum1
        
        return ratio
    
    '''Calculate gamma for the second case'''
    def cal_gamma2(self,  _d1,  _d2):
        ratio = self.cal_gamma1(_d1)
        ratio *= self.cal_gamma1(_d2)
        ratio /= self.sum_of_power[1]
        return ratio
        
    def enum_connected_subgraphs(self,  pattern_graph):
        subgraphs = []
        if(pattern_graph.number_of_edges() == 0): 
            return subgraphs
        num_edges = 1
        while(num_edges <= pattern_graph.number_of_edges()):
            for edge_set in it.combinations(pattern_graph.edges(), num_edges):
                gTmp = nx.Graph()
                gTmp.add_edges_from(edge_set)
                if(nx.is_connected(gTmp)):
                    subgraphs.append(gTmp)
                else:
                    gTmp.clear()
                    continue
            num_edges += 1
        return subgraphs
        
    def est_num_matches_random_graph(self, subg):
        m = subg.number_of_edges()
        n = subg.number_of_nodes()
        r = 2 * m - n
        num_matches = (((2 * self.num_of_data_edges) ** m) / float(self.num_of_data_vertices ** r))
        return num_matches
        
    def est_num_matches_powerlaw_graph(self, subg):
        dfs_edges = list(nx.dfs_edges(subg))
        non_dfs_edges = self.set_minus(subg.edges(), dfs_edges)
        #print(dfs_edges)
        #print(non_dfs_edges)
        gTmp = nx.Graph()
        cur_num_matches = 1
        for e in dfs_edges: #Compute case I
            if(cur_num_matches == 1):
                cur_num_matches *= (self.sum_of_power[1])
            else:
                node0, node1 = e[0], e[1]
                deg = self.get_degree(gTmp, node0)
                if(deg == 0):
                    deg = self.get_degree(gTmp, node1)
                if(deg == 0):
                    raise(Exception('We do not expect both node has degree 0.'))
                r1 = self.cal_gamma1(deg)
                cur_num_matches *= r1
            gTmp.add_edge(*e)
        for e in non_dfs_edges: #Compute case II
            node0, node1 = e[0], e[1]
            deg0 = self.get_degree(gTmp, node0)
            deg1 = self.get_degree(gTmp, node1)
            r2 = self.cal_gamma2(deg0, deg1)
            cur_num_matches *= r2
            gTmp.add_edge(*e)
        gTmp.clear()
        return cur_num_matches

    def compute_local_cost(self,  subg,  graph_model = 0,  is_join_unit = False):
        res = 0.0
        graph_size_in_page = self.num_of_data_edges * 2 / self.page_size
        if(graph_model == 0):
            subgraph_size_in_page = self.est_num_matches_random_graph(subg) / self.page_size
        else:
            subgraph_size_in_page = self.est_num_matches_powerlaw_graph(subg) / self.page_size
        if(is_join_unit):
            res = graph_size_in_page * self.unit_cost_global_mem + (2 * self.unit_cost_local_mem + self.unit_cost_com +  \
               self.unit_cost_global_mem * math.log((subgraph_size_in_page / self.buffer_size + 1),  self.merge_order)) * subgraph_size_in_page
        else:
            res = (4 * self.unit_cost_local_mem + self.unit_cost_com +  \
               self.unit_cost_global_mem * math.log((subgraph_size_in_page / self.buffer_size + 1),  self.merge_order)) * subgraph_size_in_page
        return res
        
    def optimal_dec(self,  graph_model = 1,  local_graph_type = 2):
        if(self.is_join_unit(self.pattern_graph,  local_graph_type)):
            return
        connected_subgraphs = self.enum_connected_subgraphs(self.pattern_graph)
        subgraph_labels = []
        for subg in connected_subgraphs: # Initialize the hash table
            label = self.compute_canonical_label(subg)
            subgraph_labels.append(label)
            if(not self.subgraph_hash_table.has_key(label)):
                self.subgraph_hash_table[label] = {}
                self.subgraph_hash_table[label]['graph'] = subg
                is_join_unit = self.is_join_unit(subg,  local_graph_type)
                self.subgraph_hash_table[label]['T'] = self.compute_local_cost(subg,  graph_model, is_join_unit)
                if(is_join_unit):
                    self.subgraph_hash_table[label]['C'] = 0
                else: # Initilize as an arbitrary large value
                    self.subgraph_hash_table[label]['C'] = float('inf')
                self.subgraph_hash_table[label]['lhs'] = ''
                self.subgraph_hash_table[label]['rhs'] = ''
        for subg_label in subgraph_labels:
            #print('current subgraph:',  subg_label)
            subg = self.subgraph_hash_table[subg_label]['graph']
            # When a join unit is met, we stop here
            if(self.is_join_unit(subg,  local_graph_type)):
                #print('current subgraph is a join unit.\n')
                continue
            for subg_subg in self.enum_connected_subgraphs(subg):
                subgraph_lhs = subg_subg
                subgraph_rhs = PatternDec.graph_minus(subg,  subgraph_lhs,  local_graph_type)
                # To avoid duplicate enumeration
                if(subgraph_lhs.number_of_edges() >= subgraph_rhs.number_of_edges()):
                    label_rhs = self.compute_canonical_label(subgraph_rhs)
                    if(self.subgraph_hash_table.has_key(label_rhs)): # Means subgraph_rhs is connected
                        label_lhs = self.compute_canonical_label(subgraph_lhs)
                        #print('label_lhs: ', label_lhs)
                        #print('label_rhs: ', label_rhs)
                        tmp_cost = self.subgraph_hash_table[label_lhs]['C'] + self.subgraph_hash_table[label_lhs]['T'] + \
                             self.subgraph_hash_table[label_rhs]['C'] + self.subgraph_hash_table[label_rhs]['T']
                        #print('tmp_cost: ', tmp_cost)
                        #print('cur_min_cost', self.subgraph_hash_table[subg_label]['C'])
                        if(self.subgraph_hash_table[subg_label]['C'] > tmp_cost):
                            #print('Set as min cost\n')
                            self.subgraph_hash_table[subg_label]['C'] = tmp_cost
                            self.subgraph_hash_table[subg_label]['lhs'] = label_lhs
                            self.subgraph_hash_table[subg_label]['rhs'] = label_rhs
                        #else:
                            #print('Nothing changed\n')
    
    def print_opt_dec(self):
        cur_graph_label = self.compute_canonical_label(self.pattern_graph)
        print('Optimal cost:',  self.subgraph_hash_table[cur_graph_label]['C'])
        #print(str(self.subgraph_hash_table['(1, 2)(1, 4)(2, 3)(2, 4)']))
        #print(str(self.subgraph_hash_table['(1, 3)(1, 4)']))
        self.print_dec_subtree(cur_graph_label)
        
    def print_dec_subtree(self,  label):
        if(self.subgraph_hash_table.has_key(label)):
            print(label + '->' + self.subgraph_hash_table[label]['lhs'] + ' + ' + self.subgraph_hash_table[label]['rhs'])
            self.print_dec_subtree(self.subgraph_hash_table[label]['lhs'])
            self.print_dec_subtree(self.subgraph_hash_table[label]['rhs'])
     
    @staticmethod     
    def graph_minus(graph1,  graph2,  local_graph_type = 0):
        new_graph = nx.Graph()
        new_graph.add_edges_from(graph1.edges())
        new_graph.remove_edges_from(graph2.edges())
        if(local_graph_type != 0):
            for edge in graph2.edges():
                node1,  node2 = edge[0],  edge[1]
                if(PatternDec.get_degree(new_graph,  node1) == 0 or PatternDec.get_degree(new_graph,  node2) == 0):
                    continue
                else:
                    new_graph.add_edge(node1,  node2)
        return new_graph
        
    @staticmethod
    def enum(_n,  _r):
        res = 1
        if( _n > _r ):
            for i in range(0,  _r):
                res *= (_n - i)
        return res
        
    @staticmethod
    def pow(n, r):
        res = 1.0
        if r > 0:
            for i in range(0, r):
                res *= n
        return res
        
    @staticmethod
    def set_minus(set1, set2):
        res_set = []
        seen_elem = {}
        for elem in set2:
            seen_elem[elem] = 0
        for elem in set1:
            if(not seen_elem.has_key(elem)):
                res_set.append(elem)
        return res_set
    
    @staticmethod
    def compute_canonical_label(subg):
        edges = sorted(subg.edges())
        label = ''
        for edge in edges:
            label += str(edge)
        return label
        
    @staticmethod
    def get_degree(g, node_id):
        deg = 0
        try:
            deg = nx.degree(g, node_id)
        except nx.exception.NetworkXError:
            deg = 0
        return deg
     
    @staticmethod
    def is_star(subg):
        res = False
        num_nodes = subg.number_of_nodes()
        _found = False
        for node in subg.nodes():
            if(PatternDec.get_degree(subg, node) == 1):
                continue
            elif(PatternDec.get_degree(subg, node) == num_nodes - 1):
                if(_found):
                    return res
                else:
                    _found = True
            else:
                return res
        res = True
        return res
        
    @staticmethod
    def is_VCOH(subg):
        res = False
        num_nodes = subg.number_of_nodes()
        for node in subg.nodes():
            if(PatternDec.get_degree(subg, node) == num_nodes - 1):
                res = True
                break
        return res

    @staticmethod
    def is_clique(subg):
        res = False
        num_nodes = subg.number_of_nodes()
        for node in subg.nodes():
            if(PatternDec.get_degree(subg, node) != num_nodes - 1):
                return res
        res = True
        return res
    
    @staticmethod
    def is_join_unit(subg, local_graph_type = 2):
        res = False
        num_nodes = subg.number_of_nodes()
        if(local_graph_type == 0):
            res =  PatternDec.is_star(subg)
        elif(local_graph_type == 1):
            res = PatternDec.is_VCOH(subg)
        elif(local_graph_type == 2):
            res =  PatternDec.is_clique(subg) or  PatternDec.is_star(subg)
        else:
            print >> sys.stderr, 'local_graph_type = [0 | 1 | 2]: 0 -> G_u^0, 1 -> G_u^1, 2 -> G_u^2'
            res = False
        return res

if __name__ == '__main__':
    # g is the house-like pattern
    g = nx.Graph()
    g.add_edge(1,2)
    g.add_edge(1,3)
    g.add_edge(2,3)
    g.add_edge(2,4)
    g.add_edge(3,5)
    g.add_edge(4,5)
    
    # g1 is the house-like pattern with an chordal edge in the square
    g1 = nx.Graph()
    g1.add_edge(1,2)
    g1.add_edge(1,3)
    g1.add_edge(2,3)
    g1.add_edge(2,4)
    g1.add_edge(2,5)
    g1.add_edge(3,5)
    g1.add_edge(4,5)
    
    #g2 is 4-clique
    g2 = nx.Graph()
    g2.add_edge(1,2)
    g2.add_edge(1,3)
    g2.add_edge(1,4)
    g2.add_edge(2,3)
    g2.add_edge(2,4)
    g2.add_edge(3,4)
    
    # g3 is 5-clique
    g3 = nx.Graph()
    g3.add_edge(1,2)
    g3.add_edge(1,3)
    g3.add_edge(1,4)
    g3.add_edge(1,5)
    g3.add_edge(2,3)
    g3.add_edge(2,4)
    g3.add_edge(2,5)
    g3.add_edge(3,4)
    g3.add_edge(3,5)
    g3.add_edge(4,5)
    
    # g4 is the squre with a vertex in the center connecting to all peripheral vertices
    g4 = nx.Graph()
    g4.add_edge(1,2)
    g4.add_edge(1,4)
    g4.add_edge(1,5)
    g4.add_edge(2,3)
    g4.add_edge(2,5)
    g4.add_edge(3,4)
    g4.add_edge(3,5)
    g4.add_edge(4,5)
    
    # g5 is two triangles coincide in the centrice vertex
    g5 = nx.Graph()
    g5.add_edge(1,2)
    g5.add_edge(1,5)
    g5.add_edge(2,5)
    g5.add_edge(3,4)
    g5.add_edge(3,5)
    g5.add_edge(4,5)

    # you can set other parameters. Look __init__
    pat_dec = PatternDec(10000,  30)
    pat_dec.set_pattern_graph(g5)
    # If power-law model is used, we need to generate the degree sequence so that the expected
    # number of patterns can be evaluated
    pat_dec.gen_deg_seq()
    pat_dec.cal_sum_of_power()
    #print('num_vertices = ',  pat_dec.num_of_data_vertices)
    #print('num_edges = ',  pat_dec.num_of_data_edges)
    #graph_label = pat_dec.compute_canonical_label(g2)
    #print('Optimal cost:', pat_dec.subgraph_hash_table[graph_label]['C'])
    #print(str(pat_dec.subgraph_hash_table['(1, 2)(1, 4)(2, 3)(2, 4)']))
    #print(str(pat_dec.subgraph_hash_table['(1, 3)(1, 4)']))
    
    # 0: random graph model, 1: power-law graph model
    graph_model = 1  
    # 0: G_u^0;  1: G_u^1;  2: G_u^2, Look into section 6 in the paper
    local_graph_type = 2
    pat_dec.optimal_dec(graph_model, local_graph_type)
    pat_dec.print_opt_dec()
    
    g.clear()
    g1.clear()
    g2.clear()
    g3.clear()
    g4.clear()
    g5.clear()
    


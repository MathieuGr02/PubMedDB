import logging
import os
import time
from datetime import date
import sqlite3
import json

class DataBaseHandler:
    logging.root.setLevel(logging.INFO)

    def __init__(self):
        self.db_path = None
        self.output_path = None
        self.load_json()

        self.con = None
        self.cur = None
        self.connect_db()

    def connect_db(self):
        try:
            self.con = sqlite3.connect(self.db_path)
            self.cur = self.con.cursor()
        except Exception as e:
            logging.error(e)

    def load_json(self):
        try:
            if os.path.isfile('local_config_handler.json'):
                json_file = json.load(open('local_config_handler.json'))
            else:
                json_file = json.load(open('config_handler.json'))
            for key in json_file:
                match key:
                    case 'db_path':
                        self.db_path = json_file[key]
                    case 'output_path':
                        self.output_path = json_file[key]
        except Exception as e:
            logging.error(e)

    def get_pubid_with_mesh(self, mesh_id_tuple: tuple) -> list:
        """
        Gives back all pubmed ids having annotation with specific mesh id
        :param mesh_id_tuple: Specific mesh ids
        :return: list: List of pubmed ids
        """
        where_clause = ' AND '.join(f"MeshID = \'{mesh_id}\'" for mesh_id in mesh_id_tuple)
        self.cur.execute(f'SELECT DISTINCT PubID FROM MeshAnnotation WHERE {where_clause}')
        return self.cur.fetchall()

    def get_all_annotations_of_pubmedid(self, pubid: str) -> list:
        """
        Gives back all annotations of specific pubmed id
        :param pubid: Specific pubmed id
        :return: list: List of all annotations
        """
        self.cur.execute("SELECT Category, MeshID, MeshName FROM MeshAnnotation MA JOIN Mesh ME ON MA.MeshID = ME.ID "
                         "WHERE PUBID = ?", (pubid,))
        return self.cur.fetchall()

    def get_id_name(self, id: str, id_type: str) -> str:
        """
        Gives back name of given id
        :param id: Object id
        :param id_type: Type of id (Mesh, Species, Genes)
        :return: str: Name of id
        """
        match id_type:
            case 'mesh':
                self.cur.execute('SELECT MeshName FROM Mesh WHERE ID = ?', (id,))
            case 'species':
                self.cur.execute('SELECT SpeciesName FROM Species WHERE ID = ?', (id, ))
            case 'genes':
                self.cur.execute('SELECT Symbol FROM Genes WHERE ID = ?', (id,))
        return self.cur.fetchone()[0]

    def get_related_annotations(self, category: str, mesh_id_tuple: tuple, threshold: str) -> None:
        """
        Gives back all other annotations of category by count of Pubids who have the specific meshid as an annotation
        :param category: Search Category [Disease, Chemical]
        :param mesh_id_tuple: Search Mesh IDs
        :param threshold: Minimum number of occurences
        """
        start = time.time()

        search_clause = " AND ".join(f"PubID IN (SELECT PubID FROM MeshAnnotation WHERE MeshID = \'{mesh_id}\')" for mesh_id in mesh_id_tuple)
        sql_query = f'''SELECT MeshName, count FROM Mesh M1
                        JOIN (SELECT MeshID, COUNT(MeshID) as count FROM MeshAnnotation
                            WHERE Category = ? AND {search_clause} 
                            GROUP BY MeshID
                            HAVING count >= {threshold}) M2
                        ON M1.ID = M2.MeshID
                        ORDER BY count DESC'''

        print(f"Executing Sql statement, searching for {mesh_id_tuple}")
        self.cur.execute(sql_query, (category,))
        meshcount = self.cur.fetchall()

        pubids = self.get_pubid_with_mesh(mesh_id_tuple)

        end = time.time()
        self.write_text_file(category, mesh_id_tuple, meshcount, pubids, end - start)
        logging.info(f'Time for search: {end - start}')

    # TODO: Write generic file writer for all searches
    def write_text_file(self, category: str, mesh_id_tuple: tuple, meshname_count_list: list, pubid_list: list, query_time: float) -> None:
        """
        Creates text file containing search query data
        :param category: Search category
        :param mesh_id_tuple: Search mesh ids
        :param meshname_count_list: List with meshname and occurence count
        :param pubid_list: List of all pub ids
        :param query_time: Execution time for query
        """
        meshname_list = []
        for mesh_id in mesh_id_tuple:
            self.cur.execute('SELECT MeshName FROM Mesh WHERE ID = ?', (mesh_id,))
            meshname_list.append(self.cur.fetchone()[0])

        file_name = f'{category}_{"_".join(meshname_list).replace(" ", "_")}.txt'
        logging.info(f"Writing File under name {file_name}")

        with open(f"{self.output_path}\{file_name}", 'w') as outputFile:
            outputFile.write(f"{50 * '='} \n Search results for entries:"
                             f"\n \tCategory: {category}"
                             f"\n \tMeshId: {mesh_id_tuple}"
                             f"\n \tMesh Name: {', '.join(meshname_list)}"
                             f"\n Data for search:"
                             f"\n \tSearch Date: {date.today()}"
                             f"\n \tQuery search time: {query_time:.2f} Seconds"
                             f"\n \tTotal amount of diseases found: {len(meshname_count_list)}"
                             f"\n \tTotal amount of papers found: {len(pubid_list)}"
                             f"\n{50 * '='} \n")

            for mesh in meshname_count_list:
                outputFile.write(f'{mesh[0]} ~~~ {mesh[1]}\n')

            outputFile.write(f"\n{50 * '='}")
            outputFile.write(f'\n \t Referenced PubIds:')
            outputFile.write(f"\n{50 * '='}\n")

            for pubid in pubid_list:
                outputFile.write(f'{pubid[0]}\n')


if __name__ == '__main__':
    dbh = DataBaseHandler()
    print(f'Handler for Pubmed annotation Database. \n Write help for command list.\n')
    while True:
        output = input("Enter command:")
        output_text_split = output.split(' ')
        match output_text_split[0]:
            case 'help':
                print(
                    f' Setup: command <parameter> (Without <>) -> Description \n'
                    f'Commands related to search functions: \n'
                    f'  - categories -> Gives all categories \n'
                    f'  - gna <id> <type> -> Get name of id. Type: [mesh, species, genes] \n'
                    f'  - gra <Category> - <Mesh ID> or <Mesh name> <Threshold> (Optional) -> Gives text file with all related annotations to searched mesh (Allows for multiple mesh ids) \n'
                    f'  - gap <Pubmed ID> -> Gives all annotations of an article \n'
                    f'  - gpm <Mesh ID> -> Gives all Pubmed IDs containing given mesh as annotation \n'
                    f'  - quit -> quits programm'
                )
            case 'categories':
                print(f'Categories: Disease, Chemical')
            case 'gra':
                mesh_id_tuple = tuple(mesh_id for mesh_id in output_text_split[2:-1])
                if output_text_split[-1][0] != 'D':
                    dbh.get_related_annotations(output_text_split[1], mesh_id_tuple, output_text_split[-1])
                else:
                    dbh.get_related_annotations(output_text_split[1], mesh_id_tuple, '0')
                print('file written')
            case 'gap':
                db_search_result = dbh.get_all_annotations_of_pubmedid(output_text_split[1])
                print(db_search_result)
            case 'gpm':
                mesh_id_tuple = tuple(mesh_id for mesh_id in output_text_split[2:])
                db_search_result = dbh.get_pubid_with_mesh(mesh_id_tuple)
                print(db_search_result)
            case 'gna':
                dbh.get_id_name(output_text_split[1], output_text_split[2])
            case 'quit':
                quit()

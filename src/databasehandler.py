import logging
import os
import time
from datetime import date
import sqlite3

class DataBaseHandler:
    def __init__(self):
        self.con = sqlite3.connect('pubmed.db')
        self.cur = self.con.cursor()
        self.path = os.getcwd()
        logging.root.setLevel(logging.INFO)

    def get_pubid_with_mesh(self, meshid: str) -> list:
        """
        Gives back all pubmed ids having annotation with specific mesh id
        :param meshid: Specific mesh id
        :return: list: List of pubmed ids
        """
        self.cur.execute("SELECT DISTINCT PubID FROM MeshAnnotation WHERE MeshID = ?", (meshid,))
        return self.cur.fetchall()

    def get_all_annotations_of_pubmedid(self, pubid: str) -> list:
        """
        Gives back all annotations of specific pubmed id
        :param pubid: Specific pubmed id
        :return: list: List of all annotations
        """
        self.cur.execute("SELECT * FROM MeshAnnotation WHERE PUBID = ?", (pubid,))
        return self.cur.fetchall()

    def get_id_name(self, id: str, type: str) -> str:
        """
        Gives back name of given id
        :param id: Object id
        :param type: Type of id (Mesh, Species, Genes)
        :return: str: Name of id
        """
        match type:
            case 'mesh':
                self.cur.execute('SELECT MeshName FROM Mesh WHERE ID = ?', (id,))
            case 'species':
                self.cur.execute('SELECT SpeciesName FROM Species WHERE ID = ?', (id, ))
            case 'genes':
                self.cur.execute('SELECT Symbol FROM Genes WHERE ID = ?', (id,))
        return self.cur.fetchone()[0]

    def get_related_annotations(self, category: str, meshid: str) -> None:
        """
        Gives back all other annotations of category by count of Pubids who have the specific meshid as an annotation
        :param category: Search Category [Disease, Chemical]
        :param meshid: Search Mesh ID
        """
        start = time.time()

        print(f"Executing Sql statement, searching for {meshid}")
        self.cur.execute("SELECT MeshName, count FROM Mesh M1\
                            JOIN (SELECT MeshID, COUNT(MeshID) as count FROM MeshAnnotation \
                                 WHERE Category = ? AND PubID IN (SELECT PubID FROM MeshAnnotation WHERE MeshID = ?) \
                                GROUP BY MeshID) M2\
                           ON M1.ID = M2.MeshID \
                           ORDER BY count DESC", (category, meshid,))
        meshcount = self.cur.fetchall()

        pubids = self.get_pubid_with_mesh(meshid)

        end = time.time()
        self.write_text_file(category, meshid, meshcount, pubids, end - start)
        logging.info(f'Time for search: {end - start}')


    # TODO: Write generic file writer for all searches
    def write_text_file(self, category: str, meshid: str, meshname_count_list: list, pubid_list: list, time: float) -> None:
        """
        Creates text file containing search query data
        :param category: Search category
        :param meshid: Search mesh id
        :param meshname_count_list: List with meshname and occurence count
        :param pubid_list: List of all pub ids
        :param time: Execution time for a query
        :return:
        """
        logging.info("Writing File")
        self.cur.execute("SELECT MeshName FROM Mesh WHERE ID = ?", (meshid,))
        meshname = self.cur.fetchone()
        with open(f"{self.path}\{category}_{meshname[0]}.txt", 'w') as outputFile:
            outputFile.write(f"{50 * '='} \n Search results for entries:"
                             f"\n \tCategory: {category}"
                             f"\n \tMeshId: {meshid}"
                             f"\n \tMesh Name: {meshname[0]}"
                             f"\n Data for search:"
                             f"\n \tSearch Date: {date.today()}"
                             f"\n \tQuery search time: {time:.2f} Seconds"
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
                    f'  - gra <Category> - <Mesh ID> or <Mesh name> -> Gives text file with all related annotations to searched mesh \n'
                    f'  - gap <Pubmed ID> -> Gives all annotations of a article \n'
                    f'  - gpm <Mesh ID> -> Gives all Pubmed IDs containing given mesh as annotation \n'
                    f'  - quit -> quits programm'
                )
            case 'categories':
                print(f'Categories: Disease, Chemical')
            case 'gra':
                dbh.get_related_annotations(output_text_split[1], output_text_split[2])
                print('file written')
            case 'gap':
                dbh.get_all_annotations_of_pubmedid(output_text_split[1])
            case 'gpm':
                db_search_result = dbh.get_pubid_with_mesh(output_text_split[1])
                print(db_search_result)
            case 'gna':
                dbh.get_id_name(output_text_split[1], output_text_split[2])
            case 'quit':
                quit()

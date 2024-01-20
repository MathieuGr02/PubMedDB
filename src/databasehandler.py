import logging
import time
from datetime import date

class DataBaseHandler:
    def __init__(self, conn, path):
        self.cursor = conn.cursor()
        self.path = path

    def writeTextFile(self, category: str, meshId: str, data: list, pubId: list, time : float) -> None:
        logging.info("Writing File")
        self.cursor.execute("SELECT MeshName FROM Mesh WHERE ID = ?", (meshId[5:],))
        meshname = self.cursor.fetchone()[0]
        with open(f"{self.path}\output\{category}_{meshname}.txt", 'w') as outputFile:
            outputFile.write(f"{50 * '='} \n Search results for entries:"
                             f"\n \tCategory: {category}"
                             f"\n \tMeshId: {meshId}"
                             f"\n \tMesh Name: {meshname}"
                             f"\n \tSearch Date: {date.today()}"
                             f"\n \tQuery search time: {time:.2f} Seconds"
                             f"\n \tEntries Found:{data.shape[0]}"
                             f"\n {50 * '='} \n")
            outputFile.write(data.to_string())
            outputFile.write(f"\n{50 * '='}")
            outputFile.write(f'\n \t Referenced PubIds: \n')
            outputFile.write(pubId.to_string())

    def getPubIdWithMesh(self, meshId: str):
        self.cursor.execute("SELECT PubID FROM MeshAnnotation WHERE MeshID = ?", (meshId,))
        return self.cursor.fetchone()

    def getAllAnnotationsOfPubmedId(self, pubId: int):
        self.cursor.execute("SELECT * FROM MeshAnnotation WHERE PUBID = ?", (pubId,))
        return self.cursor.fetchall()

    # Query for finding Disease and meshId annotations
    def getRelatedAnnotations(self, category: str, meshId: str) -> tuple[list, list]:
        start = time.time()

        print(f"Executing Sql statement, searching for {meshId}")
        self.cursor.execute("SELECT MeshName, count FROM Mesh M1\
                            JOIN (SELECT MeshID, COUNT(*) as count FROM MeshAnnotation \
                                 WHERE Category = 'Disease' AND PubID IN (SELECT PubID FROM MeshAnnotation WHERE MeshID = 'D012544') \
                                GROUP BY MeshID) M2\
                           ON M1.ID = M2.MeshID \
                           ORDER BY count DESC")
        meshcount = self.cursor.fetchall()
        end1 = time.time()
        logging.info(f'Fetched mesh count. Time: {end1 - start}')

        self.cursor.execute("SELECT PubID FROM MeshAnnotation WHERE MeshID = ?")
        pubids = self.cursor.fetchall()
        end2 = time.time()
        logging.info(f'Fetched pubids. Time: {end2 - end1}')
        self.writeTextFile(category, meshId, meshcount, pubids, end2 - start)
        #self.cursor.execute("SELECT M1.PubID, M3.MeshName \
        #                        FROM MeshAnnotation M1 \
        #                        JOIN MeshAnnotation M2 ON M1.PubID = M2.PubID \
         #                       JOIN Mesh M3 ON M1.MeshID = M3.ID \
         #                       WHERE M1.Category = ? AND M2.MeshID = ?",
         #                       (category, meshId[5:],))

        return meshcount, pubids

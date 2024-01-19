import logging
import os
import sqlite3
import xml.etree.ElementTree as ET

class pubmed_database:
    """
    Creates and fills all needed tables for the database
    """
    def __init__(self):
        self.cur = None
        self.data_files = f'{os.getcwd()}\datafiles'

    def __create_tables(self) -> None:
        """
        Creates the needed tables with indexes for the database
        """
        self.cur.execute('''CREATE TABLE SpeciesAnnotation 
                    (PubID INT NOT NULL CHECK (Pubid > 0),
                    SpeciesID INT,
                    FOREIGN KEY (SpeciesID) REFERENCES Species(ID)
                    )''')

        self.cur.execute('''CREATE TABLE GeneAnnotation 
                    (PubID INT NOT NULL CHECK (Pubid > 0),
                    GeneID INT,
                    FOREIGN KEY (GeneID) REFERENCES Gene(ID)
                    )''')

        self.cur.execute('''CREATE TABLE MeshAnnotation 
                    (PubID INT NOT NULL CHECK (Pubid > 0),
                    Category TEXT NOT NULL,
                    MeshID INT,
                    FOREIGN KEY (MeshID) REFERENCES Mesh(ID)      
                    )''')

        self.cur.execute('''CREATE TABLE UniqueOwnAnnotation 
                    (PubID INT NOT NULL CHECK (Pubid > 0),
                    Category TEXT NULL,
                    uniqueID INT,
                    FOREIGN KEY (uniqueID) REFERENCES UniqueOwn(ID)
                    )''')

        self.cur.execute('''CREATE TABLE Species 
                    (ID INT PRIMARY KEY NOT NULL CHECK (ID > 0),
                    SpeciesName TEXT
                    )''')

        self.cur.execute('''CREATE TABLE Mesh 
                        (ID TEXT PRIMARY KEY NOT NULL,
                        MeshName TEXT
                        )''')

        self.cur.execute('''CREATE TABLE Genes
                    (ID TEXT PRIMARY KEY NOT NULL,
                    Symbol TEXT,
                    Description TEXT
                    )''')

        self.cur.execute('''CREATE TABLE UniqueOwn
                    (ID TEXT PRIMARY KEY NOT NULL,
                    Description TEXT
                    )''')

        self.cur.execute('''CREATE INDEX MeshAnnotationIndex ON MeshAnnotation(MeshID)''')
        self.cur.execute('''CREATE INDEX PubIDAnnotationIndex ON MeshAnnotation(PubID)''')
        self.cur.execute('''CREATE INDEX GeneAnnotationIndex ON GeneAnnotation(GeneID)''')
        self.cur.execute('''CREATE INDEX MeshIndex ON Mesh(ID)''')
        self.cur.execute('''CREATE INDEX GenesIndex ON Genes(ID)''')
        self.cur.execute('''CREATE INDEX UniqueOwnIndex ON UniqueOwn(ID)''')
        self.cur.execute('''CREATE INDEX SpeciesIndex ON Species(ID)''')

    def __read_file(self) -> None:
        ignore_list = []
        logging.info(msg="Reading ignore List")
        with open('D:\mathi\[PUBTATOR]\ignore_list.txt', 'r') as textfile:
            for textline in textfile:
                ignore_list.append(textline[:-1])

        with open('D:\[DATA]\[PUBTATOR_DATA]\pubtatorDataAnnotations.txt', 'r') as textfile:
            for i, textLine in enumerate(textfile):
                if len(textLine) > 1:
                    text = textLine.split('\t')
                    match text[1]:
                        case 'Species': # All species annotations
                            self.cur.execute("INSERT INTO SpeciesAnnotation(PubID, SpeciesID) \
                                              VALUES (?, ?, ?)", (text[0], text[2],))
                        case 'Gene':    # All gene annotations
                            self.cur.execute("INSERT INTO GenesAnnotation(PubID, GeneID) \
                                              VALUES (?, ?, ?)", (text[0], text[2],))
                        case _:         # All the rest
                            if text[2] not in ignore_list and text[2][:4] == 'MESH':
                                self.cur.execute("INSERT INTO M(PubID, Category, uniqueID) \
                                           VALUES (?, ?, ?)", (text[0], text[1], text[2][5:],))
                            elif text[2] == '-':
                                self.cur.execute("INSERT INTO M(PubID, Category, uniqueID) \
                                                                           VALUES (?, ?, ?)",
                                                 (text[0], text[1], text[2][5:],))

        self.cur.commit()

    def __read_file_unique(self) -> None:
        id = 1
        with open(r'D:\[DATA]\[PUBTATOR_DATA]\pubtatorDataAnnotations.txt', 'r') as file:
            next(file)
            for i, textLine in enumerate(file):
                text = textLine.split('\t')
                if text[2] == "-":
                    result = self.conn.execute("SELECT * FROM UniqueOwn WHERE ID = ?", (f'U{id}',))
                    uniqueId = result.fetchone()
                    if uniqueId is None or len(uniqueId) == 0:
                        uniqueId = f'U{id}'
                        self.conn.execute("INSERT INTO UniqueOwn(id, description) VALUES (?, ?)",
                                          (uniqueId, text[3],))
                        id += 1
                    else:
                        uniqueId = uniqueId[0]
                    self.conn.execute("INSERT INTO UniqueOwnAnnotation(PubID, Category, uniqueID) VALUES(?, ?, ?)",
                                      (text[0], text[1], uniqueId))

        self.conn.commit()

    def __read_file_mesh(self) -> None:
        tree = ET.parse('D:\[DATA]\[PUBTATOR_DATA]\desc2023.xml')
        root = tree.getroot()
        for line in root:
            meshId = line.find("DescriptorUI").text
            meshName = line.find("DescriptorName").find("String").text
            self.cur.execute("INSERT INTO Mesh (ID, MeshName) \
                                            VALUES (?, ?)", (meshId, meshName))

        tree = ET.parse('D:\[DATA]\[PUBTATOR_DATA]\supp2023.xml')
        root = tree.getroot()
        for line in root:
            meshId = line.find("SupplementalRecordUI").text
            meshName = line.find("SupplementalRecordName").find("String").text
            self.cur.execute("INSERT INTO Mesh (ID, MeshName) \
                                            VALUES (?, ?)", (meshId, meshName))

        self.cur.commit()

    def __read_file_genes(self) -> None:
        with open(r'D:\[DATA]\[PUBTATOR_DATA]\gene_info.txt', 'r') as textFile:
            for line in textFile:
                i += 1
                if i > 1:
                    lineSplit = line.split("\t")
                    geneID, symbol, description = lineSplit[1], lineSplit[2], lineSplit[8]
                    if symbol == "NEWENTRY": x = None
                    if description.split(" ")[0] == "Record": y = None
                    self.cur.execute("INSERT INTO Genes (ID, Symbol, Description) \
                                                VALUES (?, ?, ?)", (geneID, symbol, description))

    def __read_file_species(self) -> None:
        """
        Reads the names.txt file containing the (ID, name) of species
        """
        with open(r'D:\[DATA]\[PUBTATOR_DATA]\taxdmp\names.txt', 'r') as textFile:
            for line in textFile:
                lineSplit = line.split("\t")
                lineShort = [i for i in lineSplit if i != "|"][0: -1]
                if "scientific name" in lineShort and int(lineShort[0]) > 1:
                    speciesId, speciesName = lineShort[0], lineShort[1]
                    self.cur.execute("INSERT INTO Species (ID, SpeciesName) \
                                            VALUES (?, ?)", (speciesId, speciesName))
        self.cur.commit()

    def create_database(self) -> None:
        path = os.getcwd()
        if not os.path.isfile(f'{path}\pubmed.db'):
            try:
                conn = sqlite3.connect(f'{path}\pubmed.db')
                self.cur = conn.cursor()
                logging.info(f'Succesfully connected to database: {path}\pubmed.db')
            except error as e:
                logging.error(e)

            logging.info('Creating tables')
            self.create_tables()

            logging.info('Reading files')
            self.read_file_species()

        else:
            logging.info(f'Database already exists in {path}')


if __name__ == "__main__":
    input()
    pmdb = pubmed_database()
    pmdb.create_database()
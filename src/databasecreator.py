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
        self.con = None
        self.data_files = f'{os.getcwd()}\datafiles'
        logging.root.setLevel(logging.INFO)

    def __create_tables(self) -> None:
        """
        Creates the needed tables with indexes for the database
        """
        self.cur.execute('''CREATE TABLE SpeciesAnnotation (PubID INT NOT NULL CHECK (Pubid > 0),  
                         SpeciesID INT, 
                         FOREIGN KEY (SpeciesID) 
                         REFERENCES Species(ID)
                         )''')

        self.cur.execute('''CREATE TABLE GenesAnnotation 
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

    def __create_indexes(self):
        """
        Creates indexes for tables
        """
        self.cur.execute('''CREATE INDEX MeshAnnotationIndex ON MeshAnnotation(MeshID)''')
        self.cur.execute('''CREATE INDEX PubIDAnnotationIndex ON MeshAnnotation(PubID)''')
        self.cur.execute('''CREATE INDEX GenesAnnotationIndex ON GenesAnnotation(GeneID)''')
        self.cur.execute('''CREATE INDEX SpeciesAnnotationIndex ON SpeciesAnnotation(SpeciesID)''')
        self.cur.execute('''CREATE INDEX MeshIndex ON Mesh(ID)''')
        self.cur.execute('''CREATE INDEX GenesIndex ON Genes(ID)''')
        self.cur.execute('''CREATE INDEX SpeciesIndex ON Species(ID)''')

    def __read_annotation_file(self) -> None:
        """
        Reads pubtatorDataAnnotations.txt (Pubid, category, id, ) of article annotations
        """
        ignore_list = []
        with open('D:\mathi\[PUBTATOR]\ignore_list.txt', 'r') as textfile:
            for textline in textfile:
                ignore_list.append(textline[:-1])

        try:
            with open('D:\[DATA]\[PUBTATOR_DATA]\pubtatorDataAnnotations.txt', 'r') as file:
                next(file)
                for i, textLine in enumerate(file):
                    text = textLine.split('\t')
                    match text[1]:
                        case 'Species':  # All species annotations
                            self.cur.execute("INSERT INTO SpeciesAnnotation(PubID, SpeciesID) \
                                                                  VALUES (?, ?)", (text[0], text[2],))
                        case 'Gene':  # All gene annotations
                            self.cur.execute("INSERT INTO GenesAnnotation(PubID, GeneID) \
                                                                  VALUES (?, ?)", (text[0], text[2],))
                        case 'Disease' | 'Chemical':  # All the rest
                            if text[2] not in ignore_list and text[2] != '':
                                self.cur.execute("INSERT INTO MeshAnnotation(PubID, Category, MeshID) \
                                                               VALUES (?, ?, ?)", (text[0], text[1], text[2][5:],))
                    if i % 1000000 == 0:
                        logging.info(f'At entry {i}')
                        self.con.commit()
            self.con.commit()
        except Exception as e:
            logging.error(e)

    def __read_file_mesh(self) -> None:
        """
        Reads desc2023.xml and supp2023.xml of mesh identifiers
        """
        tree = ET.parse('D:\[DATA]\[PUBTATOR_DATA]\desc2023.xml')
        root = tree.getroot()
        for line in root:
            meshId = line.find("DescriptorUI").text
            meshName = line.find("DescriptorName").find("String").text
            self.cur.execute("INSERT INTO Mesh (ID, MeshName) \
                                            VALUES (?, ?)", (meshId, meshName))
        self.con.commit()

        tree = ET.parse('D:\[DATA]\[PUBTATOR_DATA]\supp2023.xml')
        root = tree.getroot()
        for line in root:
            meshId = line.find("SupplementalRecordUI").text
            meshName = line.find("SupplementalRecordName").find("String").text
            self.cur.execute("INSERT INTO Mesh (ID, MeshName) \
                                            VALUES (?, ?)", (meshId, meshName))
        self.con.commit()

    def __read_file_genes(self) -> None:
        """
        Reads the gene_info.txt file containing (ID, symbol, description) of genes
        """
        with open(r'D:\[DATA]\[PUBTATOR_DATA]\gene_info.txt', 'r') as file:
            next(file)
            for line in file:
                lineSplit = line.split("\t")
                geneID, symbol, description = lineSplit[1], lineSplit[2], lineSplit[8]
                if symbol == "NEWENTRY": symbol = None
                if description.split(" ")[0] == "Record": description = None
                self.cur.execute("INSERT INTO Genes (ID, Symbol, Description) \
                                                                VALUES (?, ?, ?)", (geneID, symbol, description))
        self.con.commit()

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
        self.con.commit()

    def create_database(self) -> None:
        """
        Main function for creating the database
        """
        path = os.getcwd()
        if not os.path.isfile(f'{path}\pubmed.db'):
            try:
                self.con = sqlite3.connect('pubmed.db')
                self.cur = self.con.cursor()
                logging.info(f'Succesfully connected to database: {path}\pubmed.db')
            except Exception as e:
                logging.error(e)

            logging.info('Creating tables')
            self.__create_tables()

            logging.info('Reading species file')
            self.__read_file_species()
            logging.info('Reading genes file')
            self.__read_file_genes()
            logging.info('Reading mesh file')
            self.__read_file_mesh()
            logging.info('Reading annotation file')
            self.__read_annotation_file()

            #self.__create_indexes()
        else:
            logging.info(f'Database already exists in {path}')


if __name__ == "__main__":
    logging.info('Creating database')
    pmdb = pubmed_database()
    pmdb.create_database()
    logging.info('Done creating database')

class FileGeneratorIsOver(Exception):
    def __init__(self, generator):
        self.generator = generator

    def __str__(self):
        return f'Generator: {self.generator} is over!'

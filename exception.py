class GeneratorIsOver(Exception):
    def __init__(self, generator):
        self.generator = generator

    def __str__(self):
        print(f'Generator: {self.generator} is over!')

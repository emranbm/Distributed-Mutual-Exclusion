class State:
    def __init__(self, current: int, need: int):
        self.current = current
        self.need = need

    def __eq__(self, value):
        return isinstance(value, State)\
            and value.current == self.current\
            and value.need == self.need
            
    def __ne__(self, value):
        return not self == value
    
    def __repr__(self):
        return f"{{current:{self.current}, need:{self.need}}}"
    
    @property
    def extra(self):
        return self.current - self.need

    def isN(self):
        return self.current == 0 and self.need == 0

    def isR(self):
        return self.current < self.need
    
    def isE(self):
        return self.current >= self.need
    
    def isH(self):
        return self.current > 0 and self.need == 0

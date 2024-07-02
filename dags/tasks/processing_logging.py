import math


class Fraction:
    
    def __init__(self, x, y) -> None:
        self.x = x
        self.y = y
        
    def sum(self, other):
        x_new = self.x * other.y + self.y * other.x
        y_new = self.y * other.y
        return Fraction(x_new, y_new)
        
    def __str__(self) -> str:
        return f"Results: {self.x}/{self.y}"
    
        
    
def sumFraction(fraction: dict) -> Fraction:
    return Fraction(fraction["tu_1"], fraction["mau_1"]) \
        .sum(Fraction(fraction["tu_2"], fraction["mau_2"]))
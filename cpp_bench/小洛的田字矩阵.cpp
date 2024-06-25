#include <iostream>
using namespace std;

int main(){
    int n;
    cin >> n;

    for (int row = 0; row < n; row++){
        for (int col = 0; col < n; col++){
            if(col==0 || col == n-1)
                cout << "|";
            else if(row == 0 || row == n-1 || (row == n/2 && col != n/2))
                cout << "-";
            else if(col==n/2 && row != n/2)
                cout << "|";
            else
                cout << "x";
        }
        cout << endl;
    }

    return 0;
}